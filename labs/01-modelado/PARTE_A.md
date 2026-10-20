# Lab 1 · Parte A — Paso a paso: el OLTP y sus límites

Guía de trabajo del equipo para reproducir y verificar la Parte A en cualquier computador.
Sigue al pie de la letra el [README del lab](README.md); aquí solo se agregan los comandos exactos,
los problemas que encontramos en Windows y cómo verificar que todo quedó bien.

## 0. Requisitos (los del README del repo)

- Docker Desktop ≥ 4.x con WSL 2 — debe decir **Engine running**
- `uv` (Python ≥ 3.11 lo instala `uv` solo)
- Este repo clonado:
  ```bash
  git clone https://github.com/santiagovelcasallas/big-data-labs-2026-2.git
  cd big-data-labs-2026-2
  ```

### ⚠️ Dos ajustes en Windows (no cambian código del profesor)

1. **Consola y el carácter `✓`**: los scripts imprimen `✓` y la consola de Windows (cp1252) falla
   con `UnicodeEncodeError`. Solución: `PYTHONUTF8=1`.
2. **`localhost` en IPv6**: WSL (`wslrelay.exe`) ocupa `[::1]:5432`, y `localhost` resuelve a `::1`,
   así que psycopg2 falla con *server closed the connection unexpectedly*. Los scripts leen
   `DATABASE_URL`, así que basta con forzar IPv4.

Antes de correr cualquier script, en Git Bash:

```bash
export PYTHONUTF8=1
export DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/paytrack
```

En PowerShell:

```powershell
$env:PYTHONUTF8 = "1"
$env:DATABASE_URL = "postgresql://postgres:postgres@127.0.0.1:5432/paytrack"
```

## 1. Levantar PostgreSQL y pgAdmin

```bash
docker compose -f .docker/docker-compose.yml up -d postgres pgadmin
docker compose -f .docker/docker-compose.yml ps
```

✅ Debe mostrar `postgres` en `5432` y `pgadmin` en `5050`, ambos `Up`.
pgAdmin: http://localhost:5050 (usuario `admin@paytrack.co`, clave `admin`). Para registrar el servidor
dentro de pgAdmin usa host `postgres`, puerto `5432`, usuario y clave `postgres`.

## 2. Instalar dependencias y generar los datos

```bash
uv sync
uv run python labs/shared/generate_data.py --transactions 50000 --output data/
```

✅ Debe imprimir: 500 clientes, 8 comercios, 50000 transacciones y ~200 eventos de fraude.
`data/` está en `.gitignore`: cada quien genera sus datos.

> Las transacciones cubren los **últimos 90 días contados desde hoy**, y las consultas usan
> `CURRENT_DATE`. Genera, carga y toma las capturas **el mismo día**.

## 3. Cargar el OLTP

```bash
uv run python labs/01-modelado/load_oltp.py
```

✅ Termina con `¡Carga OLTP completada!`. Es idempotente: borra y recrea el esquema `oltp`.

## 4. Verificar que el OLTP quedó completo y consistente

Script propio del equipo: [`sql/verify_oltp.sql`](sql/verify_oltp.sql).

```bash
docker exec -i docker-postgres-1 psql -U postgres -d paytrack < labs/01-modelado/sql/verify_oltp.sql
```

✅ Criterios:
- Conteos iguales a los que imprimió `generate_data.py`
- La última transacción es de hoy
- Los 4 chequeos de consistencia dan **0**

Resultado con 50K (19-oct-2026), completo en [`evidencias/verify_oltp_50k.txt`](evidencias/verify_oltp_50k.txt):

| Tabla | Filas |
|---|---|
| customers | 500 |
| merchants | 8 |
| payment_methods | 5 |
| transactions | 50.000 (49.938 approved, 62 declined) |
| fraud_events | 218 (1 por transacción fraudulenta) |

Chequeos de consistencia: todos en 0. Dato curioso para el informe: las 62 transacciones
`declined` son todas fraude; los otros 156 fraudes fueron aprobados.

También se puede confirmar que el autoevaluador conecta (antes de la Parte D debe decir
`sin implementar`):

```bash
uv run python labs/01-modelado/check_queries.py
```

## 5. Planes de ejecución de las consultas OLTP

```bash
docker exec -i docker-postgres-1 psql -U postgres -d paytrack -c "ANALYZE;" -f - \
  < labs/01-modelado/sql/oltp_problems.sql > labs/01-modelado/evidencias/oltp_explain_50k.txt
```

Salida guardada en [`evidencias/oltp_explain_50k.txt`](evidencias/oltp_explain_50k.txt).

| Consulta | Tiempo | Qué domina el plan | Por qué lo obliga el 3FN |
|---|---|---|---|
| Q1 · monto por categoría (30 días) | 12,9 ms | Bitmap Heap Scan que visita las 569 páginas para leer 1/3 de las filas + Hash Join con `merchants` | `category` vive en `merchants`: hay que hacer join de cada transacción para agrupar |
| Q2 · fraude por segmento | 26,9 ms | Seq Scan de las 50.000 filas + Hash Join con `customers` (el join cuesta ~3× la lectura) | `segment` vive en `customers`; además solo guarda el valor actual (sin historia → SCD2) |
| Q3 · volumen por día/hora (7 días) | 9,3 ms | `EXTRACT()` por fila + Sort por expresión; el optimizador estima 3.846 grupos y salen 163 | Día de la semana y hora son derivados: el 3FN no los guarda, se recalculan en cada consulta |

**A 500K transacciones diarias durante un año** (~182,5 M filas, ~16 GB): Q2 recorre toda la historia
en cada ejecución (de 27 ms a minutos); Q1 y Q3 escalan con el tamaño de la ventana (15 M y 3,5 M
filas), y todo compite con las escrituras del sistema de pagos.

## Checklist Parte A

- [x] Postgres y pgAdmin arriba con el compose del repo
- [x] Datos generados (50K) y OLTP cargado
- [x] OLTP verificado (`verify_oltp.sql`, chequeos en 0)
- [x] `EXPLAIN ANALYZE` de Q1–Q3 guardado
- [ ] (Opcional, lo sugiere el README) repetir con `--transactions 500000` y comparar
- [ ] Redactar las 3 respuestas por consulta en el informe
