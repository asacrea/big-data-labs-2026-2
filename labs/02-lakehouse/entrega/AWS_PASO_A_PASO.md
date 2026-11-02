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

## 2 · Desplegar (~3 min)

```bash
cd labs/02-lakehouse/infra && serverless deploy --stage dev && cd -
```

Crea el bucket, los 5 jobs de Glue, el stream de Kinesis (**desde aquí empieza a cobrar**) y el workgroup de Athena.

## 3 · Parte A en AWS: los dos lotes (~15 min)

```bash
bash labs/02-lakehouse/run_aws.sh data/batch1 2026-09-29
bash labs/02-lakehouse/run_aws.sh data/batch2 2026-09-30
```

📸 **Captura** de la consola de Glue (*Job run monitoring*) con los 4 jobs batch en `Succeeded` para los dos lotes
→ `evidencias/aws_glue/`.

## 4 · Parte B en AWS: Glue Streaming + Kinesis (~7 min)

```bash
bash labs/02-lakehouse/run_aws.sh stream 300
```

📸 **Captura** del job `paytrack-stream` en `Succeeded` → `evidencias/aws_glue/`.
Las validaciones (`✓`) quedan en CloudWatch: `/aws-glue/jobs/output`.

## 5 · Athena: las 6 consultas

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

## 6 · Limpieza (obligatoria, el mismo día)

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
