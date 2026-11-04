#!/usr/bin/env bash
# ================================================================
# Lab 2 · Ejecución completa en AWS con salvaguardas (desde la raíz del repo, rama lab2).
#
#   bash labs/02-lakehouse/entrega/aws/ejecutar_aws.sh preflight   # solo verifica; no crea nada
#   bash labs/02-lakehouse/entrega/aws/ejecutar_aws.sh todo        # deploy → lotes → stream → Athena → limpieza
#   bash labs/02-lakehouse/entrega/aws/ejecutar_aws.sh desmontar   # limpieza manual (idempotente)
#   bash labs/02-lakehouse/entrega/aws/ejecutar_aws.sh verificar   # confirma que no queda nada
#
# Pruebas de las salvaguardas SIN desplegar el stack (crean y borran solo el kill switch):
#   bash labs/02-lakehouse/entrega/aws/ejecutar_aws.sh probar-error              # error → desmontaje
#   MAX_MIN=1 bash labs/02-lakehouse/entrega/aws/ejecutar_aws.sh probar-watchdog # watchdog → desmontaje
#   bash labs/02-lakehouse/entrega/aws/ejecutar_aws.sh probar-fail               # fallo controlado → desmontaje
#   bash labs/02-lakehouse/entrega/aws/ejecutar_aws.sh probar-disparo            # el schedule se ejecuta de verdad
#
# Prueba reducida EN AWS, para observar lo que no se puede probar en local (~25 min, ~USD 0.20):
#   bash labs/02-lakehouse/entrega/aws/ejecutar_aws.sh prueba-aws
#   deploy → lote 1 → stream → logs del stream → Athena → disparo real del kill switch → desmontaje
#
# Usa los scripts del profesor (run_aws.sh, serverless.yml) sin modificarlos. Agrega:
#   1. Verificación previa: cuenta, región, rama, datos, tests y que no exista el stack.
#   2. Desmontaje automático ante cualquier error, Ctrl+C o señal de terminación.
#   3. Watchdog local: si la ejecución pasa de MAX_MIN minutos, se detiene y desmonta.
#   4. Kill switch en AWS: un schedule de EventBridge Scheduler borra el stack KILL_MIN
#      minutos después del deploy, aunque este computador se apague. Se elimina al terminar.
#   5. Athena por CLI: DDL y las 6 consultas, con resultados y MB escaneados como evidencia.
# ================================================================
set -Eeuo pipefail

ACCOUNT=809411919665
REGION=us-east-1
STACK=paytrack-lakehouse-dev
STREAM=paytrack-transactions
JOBS=(bronze silver gold audit stream)
MAX_MIN=${MAX_MIN:-75}   # tope local de toda la ejecución (minutos)
PAUSA_MIN=${PAUSA_MIN:-10}  # pausa antes de desmontar para las capturas de Glue
KILL_MIN=120         # el kill switch en AWS borra el stack a los 120 min del deploy
KS_NAME=paytrack-killswitch
KS_ROLE=paytrack-killswitch-role

LAB=labs/02-lakehouse
EV=$LAB/entrega/evidencias
STATE_DIR="$HOME/.cache/paytrack-aws"
export MSYS_NO_PATHCONV=1 SLS_TELEMETRY_DISABLED=1 CI=1 PYTHONUTF8=1 AWS_DEFAULT_REGION=$REGION
export PATH="$PATH:/c/Program Files/Docker/Docker/resources/bin"

cd "$(git rev-parse --show-toplevel)"
mkdir -p "$STATE_DIR" "$EV/aws_glue" "$EV/athena" "$EV/limpieza"
[[ -f .envs/aws.env ]] && { set -a; source .envs/aws.env; set +a; }

ok()   { echo "  ✓ $*"; }
ARMADO=0  # 1 cuando ya puede haber recursos creados: un fallo debe desmontar
fail() { echo "  ✗ $*" >&2; [[ $ARMADO == 1 ]] && al_fallar "$LINENO"; exit 1; }
step() { echo -e "\n━━━ $* ($(date +%T)) ━━━"; }

stack_exists() { aws cloudformation describe-stacks --stack-name "$STACK" >/dev/null 2>&1; }
stack_output() { aws cloudformation describe-stacks --stack-name "$STACK" \
  --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text 2>/dev/null; }

# ── 1. Verificación previa ──────────────────────────────────────────────────
preflight() {
  step "verificación previa"
  [[ $(git branch --show-current) == lab2 ]] || fail "la rama no es lab2"; ok "rama lab2"
  [[ -n ${STUDENT_ID:-} && $STUDENT_ID =~ ^[a-z0-9-]+$ ]] || fail "STUDENT_ID vacío o inválido en .envs/aws.env"
  ok "STUDENT_ID=$STUDENT_ID"
  [[ ${AWS_ACCESS_KEY_ID:-} != your_access_key_here ]] || fail ".envs/aws.env tiene las llaves de ejemplo"
  local cuenta; cuenta=$(aws sts get-caller-identity --query Account --output text) || fail "sin credenciales de AWS"
  [[ $cuenta == "$ACCOUNT" ]] || fail "cuenta $cuenta, se esperaba $ACCOUNT"; ok "cuenta $ACCOUNT, región $REGION"
  for f in data/batch1/transactions.json data/batch2/transactions.json; do [[ -f $f ]] || fail "falta $f"; done
  ok "lotes en data/batch1 y data/batch2"
  uv run pytest -q "$LAB/tests" >/dev/null 2>&1 || fail "los tests de $LAB/tests no están en verde"; ok "tests en verde"
  command -v serverless >/dev/null || fail "falta serverless (npm i -g serverless@3)"; ok "serverless $(serverless --version 2>/dev/null | head -1)"
  stack_exists && fail "ya existe el stack $STACK: corre 'desmontar' primero"; ok "no existe el stack $STACK"
  [[ -z $(aws kinesis list-streams --query "StreamNames[?@=='$STREAM']" --output text) ]] || fail "ya existe el stream $STREAM"
  ok "no existe el stream $STREAM"
  # Grupos de logs de Glue que ya existían: el desmontaje no los toca
  aws logs describe-log-groups --log-group-name-prefix /aws-glue --query 'logGroups[].logGroupName' --output text \
    | tr '\t' '\n' | sed '/^$/d' > "$STATE_DIR/loggroups_previos.txt"
  ok "grupos de logs de Glue previos: $(wc -l < "$STATE_DIR/loggroups_previos.txt")"
}

# ── 4. Kill switch en AWS ───────────────────────────────────────────────────
crear_kill_switch() {
  step "kill switch en AWS (borra el stack en $KILL_MIN min si nadie lo hace antes)"
  aws iam create-role --role-name "$KS_ROLE" --description "Borra el stack del Lab 2 si la ejecucion local se cae" \
    --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole","Condition":{"StringEquals":{"aws:SourceAccount":"'"$ACCOUNT"'"}}}]}' \
    >/dev/null
  aws iam put-role-policy --role-name "$KS_ROLE" --policy-name borrar-stack-lab2 --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {"Effect": "Allow", "Action": ["cloudformation:DeleteStack", "cloudformation:Describe*", "cloudformation:List*"],
       "Resource": "arn:aws:cloudformation:'"$REGION:$ACCOUNT"':stack/'"$STACK"'/*"},
      {"Effect": "Allow", "Action": ["kinesis:DeleteStream", "kinesis:Describe*", "kinesis:List*"],
       "Resource": "arn:aws:kinesis:'"$REGION:$ACCOUNT"':stream/'"$STREAM"'"},
      {"Effect": "Allow", "Action": ["glue:DeleteJob", "glue:DeleteDatabase", "glue:DeleteTable", "glue:BatchDeleteTable", "glue:DeleteUserDefinedFunction", "glue:Get*"],
       "Resource": ["arn:aws:glue:'"$REGION:$ACCOUNT"':catalog", "arn:aws:glue:'"$REGION:$ACCOUNT"':database/paytrack_lakehouse",
                    "arn:aws:glue:'"$REGION:$ACCOUNT"':table/paytrack_lakehouse/*", "arn:aws:glue:'"$REGION:$ACCOUNT"':userDefinedFunction/paytrack_lakehouse/*", "arn:aws:glue:'"$REGION:$ACCOUNT"':job/paytrack-*"]},
      {"Effect": "Allow", "Action": ["iam:DeleteRole", "iam:DeleteRolePolicy", "iam:DetachRolePolicy", "iam:Get*", "iam:List*"],
       "Resource": "arn:aws:iam::'"$ACCOUNT"':role/'"$STACK"'-*"},
      {"Effect": "Allow", "Action": ["s3:DeleteBucket", "s3:DeleteBucketPolicy", "s3:Get*", "s3:List*"],
       "Resource": "arn:aws:s3:::paytrack-lakehouse-*"},
      {"Effect": "Allow", "Action": ["athena:DeleteWorkGroup", "athena:Get*", "athena:List*"],
       "Resource": "arn:aws:athena:'"$REGION:$ACCOUNT"':workgroup/paytrack-lakehouse-wg"}
    ]}' >/dev/null
  crear_schedule "$KILL_MIN"
}

crear_schedule() {  # crear_schedule <minutos>: programa el borrado del stack
  local when; when=$(date -u -d "+$1 minutes" +%Y-%m-%dT%H:%M:%S)
  # IAM tarda unos segundos en propagar el rol recién creado
  for _ in 1 2 3 4 5 6; do
    aws scheduler create-schedule --name "$KS_NAME" --schedule-expression "at($when)" --schedule-expression-timezone UTC \
      --flexible-time-window Mode=OFF --action-after-completion DELETE \
      --target '{"Arn":"arn:aws:scheduler:::aws-sdk:cloudformation:deleteStack","RoleArn":"arn:aws:iam::'"$ACCOUNT"':role/'"$KS_ROLE"'","Input":"{\"StackName\":\"'"$STACK"'\"}"}' \
      >/dev/null 2>&1 && { ok "schedule $KS_NAME: borra $STACK a las $when UTC"; return; }
    sleep 10
  done
  fail "no se pudo crear el schedule $KS_NAME"
}

borrar_kill_switch() {
  aws scheduler delete-schedule --name "$KS_NAME" >/dev/null 2>&1 && echo "  · schedule $KS_NAME borrado"
  aws iam delete-role-policy --role-name "$KS_ROLE" --policy-name borrar-stack-lab2 >/dev/null 2>&1
  aws iam delete-role --role-name "$KS_ROLE" >/dev/null 2>&1 && echo "  · rol $KS_ROLE borrado"
  return 0
}

# ── 2. Desmontaje (idempotente: se puede correr cuantas veces haga falta) ───
# Antes de borrar los grupos de logs, guarda los de las ejecuciones de Glue que fallaron:
# son la única evidencia del error y sin ellos no se puede diagnosticar.
guardar_logs_de_run() {  # guardar_logs_de_run <job> <run> <carpeta>; nunca interrumpe el desmontaje
  local job=$1 run=$2 dir=$3 g
  mkdir -p "$dir" || true
  for g in /aws-glue/jobs/error /aws-glue/jobs/output /aws-glue/jobs/logs-v2; do
    aws logs filter-log-events --log-group-name "$g" --log-stream-name-prefix "$run"       --query 'events[].message' --output text 2>/dev/null | tr '	' '
' > "$dir/${job}_${g##*/}.txt" || true
  done
  aws glue get-job-run --job-name "paytrack-$job" --run-id "$run"     --query 'JobRun.[JobRunState,ExecutionTime,ErrorMessage]' --output text > "$dir/${job}_estado.txt" 2>/dev/null || true
}

# Logs de un stream exitoso: las validaciones. El de errores (13 MB de avisos de Spark) no se conserva.
guardar_logs_stream() {
  local dir=$1 run
  paso sleep 30  # CloudWatch tarda unos segundos en recibir los últimos eventos
  run=$(aws glue get-job-runs --job-name paytrack-stream --max-results 1 --query 'JobRuns[0].Id' --output text)
  guardar_logs_de_run stream "$run" "$dir"
  rm -f "$dir/stream_error.txt" "$dir/stream_logs-v2.txt"
  grep -hE "Columnas del conector|no agregó|Stream listo|^✗" "$dir"/stream_*.txt | sort -u || true
}

guardar_logs_de_fallos() {
  local dir job runs run
  dir="$EV/aws_glue/logs_fallos_$(date +%Y%m%d_%H%M)"
  for job in "${JOBS[@]}"; do
    runs=$(aws glue get-job-runs --job-name "paytrack-$job" --max-results 20       --query "JobRuns[?JobRunState=='FAILED'||JobRunState=='ERROR'||JobRunState=='TIMEOUT'].Id" --output text 2>/dev/null) || true
    [[ -z $runs || $runs == None ]] && continue
    for run in $runs; do guardar_logs_de_run "$job" "$run" "$dir"; done
    echo "  · logs del fallo de paytrack-$job guardados en $dir"
  done
  return 0
}

DESMONTANDO=0
desmontar() {
  [[ $DESMONTANDO == 1 ]] && return 0
  DESMONTANDO=1
  set +e
  trap - ERR
  step "desmontaje"
  guardar_logs_de_fallos
  for job in "${JOBS[@]}"; do  # detener ejecuciones de Glue que sigan corriendo
    local runs
    runs=$(aws glue get-job-runs --job-name "paytrack-$job" --max-results 20 \
      --query "JobRuns[?JobRunState=='RUNNING'||JobRunState=='STARTING'||JobRunState=='WAITING'].Id" --output text 2>/dev/null)
    [[ -n $runs && $runs != None ]] && aws glue batch-stop-job-run --job-name "paytrack-$job" --job-run-ids $runs >/dev/null \
      && echo "  · detenido paytrack-$job ($runs)"
  done
  if stack_exists; then
    local bucket deploy_bucket
    bucket=$(aws cloudformation describe-stack-resource --stack-name "$STACK"       --logical-resource-id LakehouseBucket --query StackResourceDetail.PhysicalResourceId --output text 2>/dev/null)
    deploy_bucket=$(aws cloudformation describe-stack-resource --stack-name "$STACK" \
      --logical-resource-id ServerlessDeploymentBucket --query StackResourceDetail.PhysicalResourceId --output text 2>/dev/null)
    for b in $bucket $deploy_bucket; do
      [[ -n $b && $b != None ]] && aws s3 rm "s3://$b" --recursive --quiet && echo "  · bucket $b vaciado"
    done
    aws cloudformation delete-stack --stack-name "$STACK"
    echo "  · borrando el stack $STACK (puede tardar unos minutos)"
    aws cloudformation wait stack-delete-complete --stack-name "$STACK"
  fi
  borrar_kill_switch
  # Grupos de logs de Glue creados por esta ejecución (los que no existían antes)
  local previos="$STATE_DIR/loggroups_previos.txt"
  if [[ -f $previos ]]; then
    for g in $(aws logs describe-log-groups --log-group-name-prefix /aws-glue --query 'logGroups[].logGroupName' --output text); do
      grep -qxF "$g" "$previos" || { aws logs delete-log-group --log-group-name "$g" && echo "  · log group $g borrado"; }
    done
  fi
  verificar
}

verificar() {
  step "verificación de la limpieza"
  local limpio=1
  stack_exists && { echo "  ✗ el stack $STACK sigue existiendo"; limpio=0; } || ok "sin stack $STACK"
  [[ -z $(aws kinesis list-streams --query "StreamNames[?@=='$STREAM']" --output text) ]] \
    && ok "sin stream de Kinesis $STREAM" || { echo "  ✗ el stream $STREAM sigue existiendo"; limpio=0; }
  [[ -z $(aws s3 ls | grep paytrack-lakehouse) ]] && ok "sin buckets paytrack-lakehouse-*" \
    || { echo "  ✗ quedan buckets paytrack-lakehouse-*"; limpio=0; }
  [[ -z $(aws glue get-jobs --query "Jobs[?starts_with(Name, 'paytrack-')].Name" --output text) ]] \
    && ok "sin jobs de Glue paytrack-*" || { echo "  ✗ quedan jobs de Glue paytrack-*"; limpio=0; }
  aws scheduler get-schedule --name "$KS_NAME" >/dev/null 2>&1 && { echo "  ✗ el schedule $KS_NAME sigue existiendo"; limpio=0; } \
    || ok "sin schedule $KS_NAME"
  aws iam get-role --role-name "$KS_ROLE" >/dev/null 2>&1 && { echo "  ✗ el rol $KS_ROLE sigue existiendo"; limpio=0; } \
    || ok "sin rol $KS_ROLE"
  [[ $limpio == 1 ]] && echo "  → AWS limpio" || echo "  → REVISAR: quedaron recursos (correr 'desmontar' de nuevo)"
}

# ── Manejo de errores, señales y watchdog ───────────────────────────────────
WATCHDOG_PID=""
al_fallar() {
  local rc=$? linea=$1
  ARMADO=0
  # Primero desactivar errexit e ignorar señales: nada debe cortar el desmontaje
  set +e; trap - ERR; trap '' INT TERM
  echo -e "\n✗ ERROR en la línea $linea (código $rc): '$BASH_COMMAND'. Se desmonta todo para no gastar." >&2
  parar_watchdog
  desmontar
  echo "Para reiniciar: corrige el error y vuelve a correr 'todo' (empieza de cero)." >&2
  exit 1
}
al_interrumpir() {
  ARMADO=0
  set +e; trap - ERR; trap '' INT TERM
  echo -e "\n✗ Ejecución interrumpida (Ctrl+C, señal o watchdog de $MAX_MIN min). Se desmonta todo." >&2
  parar_watchdog
  jobs -p | xargs -r kill 2>/dev/null
  desmontar
  exit 130
}
iniciar_watchdog() {
  local padre=$$
  (
    # El watchdog no hereda el manejo de errores del script, y al detenerlo detiene su sleep
    set +eE; trap - ERR INT
    trap 'kill "$s" 2>/dev/null; exit 0' TERM
    sleep $((MAX_MIN * 60)) & s=$!
    wait "$s" || exit 0
    echo -e "\n⏱ watchdog: $MAX_MIN min cumplidos" >&2
    kill -TERM "$padre"
  ) &
  WATCHDOG_PID=$!
}
parar_watchdog() { [[ -n $WATCHDOG_PID ]] && kill "$WATCHDOG_PID" 2>/dev/null; WATCHDOG_PID=""; return 0; }
# Los pasos largos corren en segundo plano y se esperan con `wait`: así una señal
# interrumpe de inmediato en vez de esperar a que termine el comando en curso.
paso() { "$@" & wait $!; }

estado_jobs() {  # evidencia en texto: estado final de cada ejecución de Glue
  for job in "${JOBS[@]}"; do
    aws glue get-job-runs --job-name "paytrack-$job" \
      --query 'JobRuns[].[JobName,JobRunState,StartedOn,ExecutionTime,Arguments."--INGEST_DATE"]' --output text 2>/dev/null
  done | sort -k3 > "$EV/aws_glue/estado_jobs.txt"
  cat "$EV/aws_glue/estado_jobs.txt"
}

stream_a_50() {
  local run_id estado
  run_id=$(aws glue start-job-run --job-name paytrack-stream --arguments '{"--RUN_SECONDS":"300"}' --query JobRunId --output text)
  echo "  · paytrack-stream lanzado ($run_id)"
  paso uv run python "$LAB/producer.py" --sink kinesis --rate 50 --duration 240 2>&1 | tr '' '
' | tail -1
  while true; do
    estado=$(aws glue get-job-run --job-name paytrack-stream --run-id "$run_id" --query JobRun.JobRunState --output text)
    case "$estado" in
      STARTING|RUNNING|WAITING|STOPPING) paso sleep 15 ;;
      SUCCEEDED) ok "paytrack-stream a 50 eventos/s: $estado"; return 0 ;;
      *) fail "paytrack-stream a 50 eventos/s: $estado" ;;
    esac
  done
}

todo() {
  local log; log="$EV/aws_glue/ejecucion_$(date +%Y%m%d_%H%M).txt"
  exec > >(tee -a "$log") 2>&1
  echo "Lab 2 en AWS · $(date '+%Y-%m-%d %H:%M') · log: $log"
  preflight
  trap 'al_fallar $LINENO' ERR
  trap al_interrumpir INT TERM
  ARMADO=1
  iniciar_watchdog
  crear_kill_switch

  step "deploy"
  pushd "$LAB/infra" >/dev/null; paso serverless deploy --stage dev; popd >/dev/null
  BUCKET=$(stack_output LakehouseBucket); ok "bucket $BUCKET"

  step "Parte A · lote 1";  paso bash "$LAB/run_aws.sh" data/batch1 2026-09-29
  step "Parte A · lote 2";  paso bash "$LAB/run_aws.sh" data/batch2 2026-09-30
  step "Parte B · stream";  paso bash "$LAB/run_aws.sh" stream 300
  step "logs del stream a 20 eventos/s"; guardar_logs_stream "$EV/aws_glue/stream_logs_rate20"

  step "Athena (stream a 20 eventos/s)"
  paso uv run python "$LAB/entrega/aws/athena_cli.py" --bucket "$BUCKET" --out "$EV/athena/athena_resultados.md"

  # Experimento 2 del README: precisión también a 50 eventos/s. run_aws.sh tiene fija la tasa de 20.
  # Las alertas se acumulan: la precisión a 50 sale de la diferencia entre los dos TODO 5.
  step "Parte B · stream a 50 eventos/s"; stream_a_50
  step "logs del stream a 50 eventos/s"; guardar_logs_stream "$EV/aws_glue/stream_logs_rate50"
  step "Athena (acumulado con el stream a 50 eventos/s)"
  paso uv run python "$LAB/entrega/aws/athena_cli.py" --bucket "$BUCKET" --out "$EV/athena/athena_resultados_acumulado_rate50.md"
  step "estado de los jobs"; estado_jobs

  # El desmontaje borra los jobs y su historial: las capturas de Glue se toman antes.
  step "PAUSA de $PAUSA_MIN min: toma ahora las capturas de Glue (jobs en Succeeded)"
  paso sleep $((PAUSA_MIN * 60))

  parar_watchdog
  ARMADO=0
  trap - ERR INT TERM
  desmontar
  echo -e "\n✓ Ejecución completa · $(date +%T). Evidencias en $EV/aws_glue y $EV/athena."
}

# Dispara el kill switch de verdad y comprueba que el stream de Kinesis desaparezca.
# Se espera DELETE_FAILED solo en los dos buckets: CloudFormation no borra buckets con objetos.
probar_kill_switch() {
  local out=$1 estado="" stream="" fallidos
  step "disparo real del kill switch (en ~2 min)"
  aws scheduler delete-schedule --name "$KS_NAME"
  crear_schedule 2
  for _ in $(seq 1 48); do  # hasta 12 minutos
    paso sleep 15
    estado=$(aws cloudformation describe-stacks --stack-name "$STACK" --query 'Stacks[0].StackStatus' --output text 2>/dev/null) || estado=BORRADO
    stream=$(aws kinesis list-streams --query "StreamNames[?@=='$STREAM']" --output text) || stream=desconocido
    echo "  · $(date +%T) stack: $estado · stream: ${stream:-borrado}"
    [[ -z $stream && $estado != DELETE_IN_PROGRESS ]] && break
  done
  fallidos=$(aws cloudformation describe-stack-events --stack-name "$STACK"     --query "StackEvents[?ResourceStatus=='DELETE_FAILED'].[LogicalResourceId,ResourceStatusReason]" --output text 2>/dev/null) || fallidos=""
  { echo "Disparo real del kill switch · $(date '+%Y-%m-%d %H:%M')"; echo "estado final del stack: $estado"
    echo "stream de Kinesis: ${stream:-borrado}"; echo "recursos con DELETE_FAILED:"; echo "$fallidos"; } | tee "$out"
  [[ -z $stream ]] || fail "el kill switch no borró el stream de Kinesis"
  ok "el kill switch borró el stream de Kinesis"
  if grep -vE '^(LakehouseBucket|ServerlessDeploymentBucket|paytrack-lakehouse-dev)' <<< "$fallidos" | grep -q .; then
    echo "  ⚠ el kill switch dejó otros recursos: revisar los permisos de su rol ($out)"
  fi
}

# Prueba reducida en AWS: observa lo que en local no se puede probar. Si algo falla, desmonta y conserva los logs.
prueba_aws() {
  local log dir
  log="$EV/aws_glue/prueba_$(date +%Y%m%d_%H%M).txt"
  dir="$EV/aws_glue/prueba_logs_$(date +%Y%m%d_%H%M)"
  exec > >(tee -a "$log") 2>&1
  echo "Lab 2 · prueba reducida en AWS · $(date '+%Y-%m-%d %H:%M') · log: $log"
  MAX_MIN=45
  preflight
  trap 'al_fallar $LINENO' ERR
  trap al_interrumpir INT TERM
  ARMADO=1
  iniciar_watchdog
  crear_kill_switch

  step "deploy"
  pushd "$LAB/infra" >/dev/null; paso serverless deploy --stage dev; popd >/dev/null
  BUCKET=$(stack_output LakehouseBucket); ok "bucket $BUCKET"

  step "Parte A · lote 1"; paso bash "$LAB/run_aws.sh" data/batch1 2026-09-29
  step "Parte B · stream"; paso bash "$LAB/run_aws.sh" stream 300
  step "estado de los jobs"; estado_jobs

  step "logs del stream"
  guardar_logs_stream "$dir"

  step "Athena: DDL y las 6 consultas"
  paso uv run python "$LAB/entrega/aws/athena_cli.py" --bucket "$BUCKET" --out "$EV/athena/prueba_athena_resultados.md"

  probar_kill_switch "$dir/kill_switch.txt"

  parar_watchdog
  ARMADO=0
  trap - ERR INT TERM
  desmontar
  echo -e "
✓ Prueba reducida completa · $(date +%T). Evidencias en $dir y $log."
}

# Pruebas: mismo manejo de errores y señales que 'todo', pero sin stack
probar() {
  preflight
  trap 'al_fallar $LINENO' ERR
  trap al_interrumpir INT TERM
  ARMADO=1
  iniciar_watchdog
  crear_kill_switch
  aws scheduler get-schedule --name "$KS_NAME" --query '{hora:ScheduleExpression,objetivo:Target.Arn,entrada:Target.Input}' --output table
  if [[ $1 == error ]]; then
    step "simulando un error en un paso"
    paso false
  elif [[ $1 == fail ]]; then
    step "simulando un fallo controlado (fail) con recursos ya creados"
    fail "fallo simulado"
  elif [[ $1 == disparo ]]; then
    # Gratis y sin stack: DeleteStack sobre un stack inexistente no hace nada, pero prueba que el
    # schedule se ejecuta con su rol. Al ejecutarse, AWS lo borra (--action-after-completion DELETE).
    step "disparando el schedule de verdad, sin stack"
    aws scheduler delete-schedule --name "$KS_NAME"
    crear_schedule 2
    for _ in $(seq 1 28); do
      paso sleep 15
      aws scheduler get-schedule --name "$KS_NAME" >/dev/null 2>&1 || { ok "el schedule se ejecutó y AWS lo borró ($(date +%T))"; break; }
    done
    aws scheduler get-schedule --name "$KS_NAME" >/dev/null 2>&1 && fail "el schedule no se ejecutó en 7 minutos"
    parar_watchdog; ARMADO=0; trap - ERR INT TERM; desmontar; return 0
  else
    step "simulando un paso colgado: el watchdog debe cortarlo a los $MAX_MIN min"
    paso sleep $((MAX_MIN * 60 + 120))
  fi
  echo "✗ la prueba no disparó el desmontaje" >&2; exit 1
}

case "${1:-}" in
  preflight) preflight ;;
  probar-error)    probar error ;;
  probar-fail)     probar fail ;;
  probar-disparo)  probar disparo ;;
  probar-watchdog) probar watchdog ;;
  todo)      todo ;;
  prueba-aws) prueba_aws ;;
  desmontar) desmontar ;;
  verificar) verificar ;;
  *) echo "uso: $0 preflight | todo | desmontar | verificar | prueba-aws | probar-error | probar-fail | probar-disparo | probar-watchdog" >&2; exit 2 ;;
esac
