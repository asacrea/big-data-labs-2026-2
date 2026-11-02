# Lab 2 · Ejecución en AWS, paso a paso

Todo en **un solo día y de corrido**: Kinesis cobra por hora mientras el stream exista (~USD 0,72 al día con
2 shards), aunque no le llegue nada. Costo esperado de la jornada completa: **~USD 2** (estimado del enunciado).

Los comandos son de **Git Bash** (los scripts del lab son bash) y se corren **desde la raíz del repositorio**,
en la rama `lab2`.

## 0 · Antes de empezar (ya verificado el 2026-11-02)

| Requisito | Estado |
|---|---|
| Parte A y Parte B en verde en local | ✅ (`evidencias/a_batch_local/`, `evidencias/b_streaming_local/`) |
| Lotes generados en `data/batch1` y `data/batch2` | ✅ |
| AWS CLI v2 | ✅ 2.34.64 |
| Serverless Framework v3 | ✅ 3.40.0 (`npm i -g serverless@3`) |
| `serverless.yml` se resuelve sin errores (`serverless print`) | ✅ |
| `boto3` en el entorno de `uv` (lo usa el productor para Kinesis) | ✅ |
| Ningún recurso de PayTrack previo en la cuenta | ✅ (sin stack, sin streams, sin jobs, sin bucket) |

## 1 · Credenciales (las pone cada uno, nunca se suben a git)

```bash
cd /c/Users/santi/big-data-labs-2026-2
git branch --show-current          # debe decir lab2
cp .envs/aws.env.example .envs/aws.env
```

Editar `.envs/aws.env`:
- `STUDENT_ID`: un nombre corto en minúsculas, sin espacios. Va en el nombre del bucket: `paytrack-lakehouse-<STUDENT_ID>-<cuenta>`.
- `AWS_ACCESS_KEY_ID` y `AWS_SECRET_ACCESS_KEY`: las de la cuenta que se use para el lab. Si se usan las que ya
  tiene configuradas la AWS CLI, **borrar esas dos líneas** del archivo para que no las reemplacen con los valores de ejemplo.

`.envs/*.env` está en `.gitignore`. Cargar y comprobar la cuenta:

```bash
set -a && source .envs/aws.env && set +a
aws sts get-caller-identity --query '{cuenta:Account, usuario:Arn}' --output table
```

## 2 · Ejecución protegida (recomendada): un solo comando

```bash
bash labs/02-lakehouse/entrega/aws/ejecutar_aws.sh preflight   # verifica todo sin crear nada
bash labs/02-lakehouse/entrega/aws/ejecutar_aws.sh todo        # ~45 min, de punta a punta
```

`todo` usa los scripts del profesor (`serverless.yml`, `run_aws.sh`) sin modificarlos, en este orden:
verificación previa → kill switch → deploy → lote 1 → lote 2 → stream → estado de los jobs → Athena → limpieza.

| Salvaguarda | Qué hace | Probada sin desplegar (2026-11-02) |
|---|---|---|
| Verificación previa | Aborta sin crear nada si la cuenta no es 809411919665, la rama no es `lab2`, faltan los lotes, los tests no están en verde, falta `STUDENT_ID` o ya existe el stack | ✅ |
| Desmontaje ante error | Cualquier error, Ctrl+C o señal detiene los jobs de Glue, vacía los buckets, borra el stack, el kill switch y los logs de Glue creados, y verifica que no quede nada | ✅ `probar-error` |
| Watchdog local (75 min) | Si la ejecución se cuelga o se alarga, la corta y desmonta | ✅ `probar-watchdog` con 1 min |
| Kill switch en AWS (120 min) | Schedule de EventBridge Scheduler que borra el stack desde AWS aunque este computador se apague. Se borra al terminar | ✅ creado y borrado en las dos pruebas |
| Athena por CLI | El DDL y las 6 consultas, con resultados y MB escaneados en `evidencias/athena/athena_resultados.md` | se prueba en el deploy |

Recursos que crea el script y no están en la plantilla del profesor: solo el schedule `paytrack-killswitch` y su rol IAM
`paytrack-killswitch-role` (gratuitos; se borran al terminar o en el desmontaje).

**Si algo falla:** el script desmonta solo y dice en qué línea falló. Se corrige y se vuelve a correr `todo` desde cero.
Limpieza manual en cualquier momento: `ejecutar_aws.sh desmontar` (idempotente) y `ejecutar_aws.sh verificar`.

📸 Mientras corre: captura de la consola de Glue con los 4 jobs batch en `Succeeded` para los dos lotes y del job
`paytrack-stream` en `Succeeded` → `evidencias/aws_glue/`. El script también guarda `estado_jobs.txt` y el log completo.

📸 Al final: captura de la consola de Kinesis sin streams y de CloudFormation sin el stack → `evidencias/limpieza/`.

---

## Anexo · Los mismos pasos a mano (sin salvaguardas)

### Desplegar (~3 min)

```bash
cd labs/02-lakehouse/infra && serverless deploy --stage dev && cd -
```

Crea el bucket, los 5 jobs de Glue, el stream de Kinesis (**desde aquí empieza a cobrar**) y el workgroup de Athena.

### Parte A en AWS: los dos lotes (~15 min)

```bash
bash labs/02-lakehouse/run_aws.sh data/batch1 2026-09-29
bash labs/02-lakehouse/run_aws.sh data/batch2 2026-09-30
```

📸 **Captura** de la consola de Glue (*Job run monitoring*) con los 4 jobs batch en `Succeeded` para los dos lotes
→ `evidencias/aws_glue/`.

### Parte B en AWS: Glue Streaming + Kinesis (~7 min)

```bash
bash labs/02-lakehouse/run_aws.sh stream 300
```

📸 **Captura** del job `paytrack-stream` en `Succeeded` → `evidencias/aws_glue/`.
Las validaciones (`✓`) quedan en CloudWatch: `/aws-glue/jobs/output`.

### Athena: las 6 consultas

1. Nombre del bucket:
   ```bash
   aws cloudformation describe-stacks --stack-name paytrack-lakehouse-dev \
     --query "Stacks[0].Outputs[?OutputKey=='LakehouseBucket'].OutputValue" --output text
   ```
2. Consola de Athena → workgroup **`paytrack-lakehouse-wg`**.
3. Ejecutar el DDL de `sql/athena.sql` (los `CREATE EXTERNAL TABLE`), reemplazando `<BUCKET>`.
4. Ejecutar los TODO 1 a 6, uno por uno.

📸 Por cada consulta: **resultado + MB escaneados** (pestaña *Query stats*) → `evidencias/athena/`.

Referencia local (Spark sobre `lakehouse-local`): `evidencias/athena/validacion_local_spark.txt`.
En AWS los números cambian porque el stream genera otros eventos. El TODO 4 debe mostrar los contracargos del
lote 2: en local fueron 276 por COP 23.933.218,70.

### Limpieza (obligatoria, el mismo día)

```bash
BUCKET=$(aws cloudformation describe-stacks --stack-name paytrack-lakehouse-dev \
  --query "Stacks[0].Outputs[?OutputKey=='LakehouseBucket'].OutputValue" --output text)
aws s3 rm s3://$BUCKET --recursive
cd labs/02-lakehouse/infra && serverless remove --stage dev && cd -
```

Verificar que no quede nada:

```bash
aws kinesis list-streams --query StreamNames --output text                      # vacío
aws cloudformation describe-stacks --stack-name paytrack-lakehouse-dev          # "does not exist"
aws s3 ls | grep paytrack                                                       # vacío
```

📸 **Capturas** de la consola de Kinesis sin streams y de CloudFormation sin el stack → `evidencias/limpieza/`.
