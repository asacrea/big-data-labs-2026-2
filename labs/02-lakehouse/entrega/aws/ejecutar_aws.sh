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
fail() { echo "  ✗ $*" >&2; exit 1; }
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
      {"Effect": "Allow", "Action": ["glue:DeleteJob", "glue:DeleteDatabase", "glue:DeleteTable", "glue:BatchDeleteTable", "glue:Get*"],
       "Resource": ["arn:aws:glue:'"$REGION:$ACCOUNT"':catalog", "arn:aws:glue:'"$REGION:$ACCOUNT"':database/paytrack_lakehouse",
                    "arn:aws:glue:'"$REGION:$ACCOUNT"':table/paytrack_lakehouse/*", "arn:aws:glue:'"$REGION:$ACCOUNT"':job/paytrack-*"]},
      {"Effect": "Allow", "Action": ["iam:DeleteRole", "iam:DeleteRolePolicy", "iam:DetachRolePolicy", "iam:Get*", "iam:List*"],
       "Resource": "arn:aws:iam::'"$ACCOUNT"':role/'"$STACK"'-*"},
      {"Effect": "Allow", "Action": ["s3:DeleteBucket", "s3:DeleteBucketPolicy", "s3:Get*", "s3:List*"],
       "Resource": "arn:aws:s3:::paytrack-lakehouse-*"},
      {"Effect": "Allow", "Action": ["athena:DeleteWorkGroup", "athena:Get*", "athena:List*"],
       "Resource": "arn:aws:athena:'"$REGION:$ACCOUNT"':workgroup/paytrack-lakehouse-wg"}
    ]}' >/dev/null
  local when; when=$(date -u -d "+$KILL_MIN minutes" +%Y-%m-%dT%H:%M:%S)
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
guardar_logs_de_fallos() {
  local dir job runs
  dir="$EV/aws_glue/logs_fallos_$(date +%Y%m%d_%H%M)"
  for job in "${JOBS[@]}"; do
    runs=$(aws glue get-job-runs --job-name "paytrack-$job" --max-results 20 \
      --query "JobRuns[?JobRunState=='FAILED'||JobRunState=='ERROR'||JobRunState=='TIMEOUT'].Id" --output text 2>/dev/null)
    [[ -z $runs || $runs == None ]] && continue
    mkdir -p "$dir"
    for run in $runs; do
      for g in /aws-glue/jobs/error /aws-glue/jobs/output; do
        aws logs filter-log-events --log-group-name "$g" --log-stream-name-prefix "$run" \
          --query 'events[].message' --output text 2>/dev/null | tr '\t' '\n' > "$dir/${job}_${g##*/}.txt"
      done
      aws glue get-job-run --job-name "paytrack-$job" --run-id "$run" \
        --query 'JobRun.[JobRunState,ErrorMessage]' --output text > "$dir/${job}_estado.txt" 2>/dev/null
    done
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
    bucket=$(stack_output LakehouseBucket)
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
  # Primero desactivar errexit e ignorar señales: nada debe cortar el desmontaje
  set +e; trap - ERR; trap '' INT TERM
  echo -e "\n✗ ERROR en la línea $linea (código $rc): '$BASH_COMMAND'. Se desmonta todo para no gastar." >&2
  parar_watchdog
  desmontar
  echo "Para reiniciar: corrige el error y vuelve a correr 'todo' (empieza de cero)." >&2
  exit 1
}
al_interrumpir() {
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

todo() {
  local log; log="$EV/aws_glue/ejecucion_$(date +%Y%m%d_%H%M).txt"
  exec > >(tee -a "$log") 2>&1
  echo "Lab 2 en AWS · $(date '+%Y-%m-%d %H:%M') · log: $log"
  preflight
  trap 'al_fallar $LINENO' ERR
  trap al_interrumpir INT TERM
  iniciar_watchdog
  crear_kill_switch

  step "deploy"
  pushd "$LAB/infra" >/dev/null; paso serverless deploy --stage dev; popd >/dev/null
  BUCKET=$(stack_output LakehouseBucket); ok "bucket $BUCKET"

  step "Parte A · lote 1";  paso bash "$LAB/run_aws.sh" data/batch1 2026-09-29
  step "Parte A · lote 2";  paso bash "$LAB/run_aws.sh" data/batch2 2026-09-30
  step "Parte B · stream";  paso bash "$LAB/run_aws.sh" stream 300
  step "estado de los jobs"; estado_jobs

  step "Athena"
  paso uv run python "$LAB/entrega/aws/athena_cli.py" --bucket "$BUCKET" --out "$EV/athena/athena_resultados.md"

  parar_watchdog
  trap - ERR INT TERM
  desmontar
  echo -e "\n✓ Ejecución completa · $(date +%T). Evidencias en $EV/aws_glue y $EV/athena."
}

# Pruebas: mismo manejo de errores y señales que 'todo', pero sin stack
probar() {
  preflight
  trap 'al_fallar $LINENO' ERR
  trap al_interrumpir INT TERM
  iniciar_watchdog
  crear_kill_switch
  aws scheduler get-schedule --name "$KS_NAME" --query '{hora:ScheduleExpression,objetivo:Target.Arn,entrada:Target.Input}' --output table
  if [[ $1 == error ]]; then
    step "simulando un error en un paso"
    paso false
  else
    step "simulando un paso colgado: el watchdog debe cortarlo a los $MAX_MIN min"
    paso sleep $((MAX_MIN * 60 + 120))
  fi
  echo "✗ la prueba no disparó el desmontaje" >&2; exit 1
}

case "${1:-}" in
  preflight) preflight ;;
  probar-error)    probar error ;;
  probar-watchdog) probar watchdog ;;
  todo)      todo ;;
  desmontar) desmontar ;;
  verificar) verificar ;;
  *) echo "uso: $0 preflight | todo | desmontar | verificar | probar-error | probar-watchdog" >&2; exit 2 ;;
esac
