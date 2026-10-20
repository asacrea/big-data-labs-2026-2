# Lab 1 · Parte A — Paso a paso: el OLTP y sus límites

Bitácora del equipo. Sigue al pie de la letra la Parte A del [README del lab](README.md) (pasos 1–5)
y deja la evidencia de cada paso en [`evidencias/`](evidencias/). Las respuestas redactadas están en
el [borrador del informe](informe/informe_lab1.md).

**Estado: ✅ Parte A ejecutada y verificada (19-oct-2026), con 50K y con 500K transacciones.**

## Cómo reproducirla

Requisitos (los del README del repo): Docker Desktop ≥ 4.x con WSL 2 en *Engine running*, y `uv`.

```bash
git clone https://github.com/santiagovelcasallas/big-data-labs-2026-2.git
cd big-data-labs-2026-2
bash labs/01-modelado/run_parte_a.sh           # pasos 1–4 con 50.000 transacciones
bash labs/01-modelado/run_parte_a.sh 500000    # prueba de escala que sugiere el README
```

[`run_parte_a.sh`](run_parte_a.sh) corre exactamente los comandos del README y guarda la salida de cada
paso en `evidencias/parte_a_<N>/`. No modifica ningún archivo del profesor. Si se prefiere a mano,
los comandos están abajo, paso por paso.

> ⚠️ **Termina siempre cargando 50K** (`run_parte_a.sh` sin argumento): es el volumen del README y el
> que se usa en las Partes B–D.

### Dos ajustes en Windows (no cambian código del profesor)

1. **`UnicodeEncodeError` con `✓`**: la consola de Windows (cp1252) no imprime ese carácter. Solución: `PYTHONUTF8=1`.
2. **`server closed the connection unexpectedly`**: WSL (`wslrelay.exe`) ocupa `[::1]:5432`, y `localhost`
   resuelve a IPv6. Los scripts leen `DATABASE_URL`, así que se fuerza IPv4.

El script ya los aplica. A mano, en Git Bash:

```bash
export PYTHONUTF8=1
export DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/paytrack
```

---

## Paso 1 · Levantar PostgreSQL y pgAdmin

```bash
docker compose -f .docker/docker-compose.yml up -d postgres pgadmin
```

✅ `postgres` en el puerto 5432 y `pgadmin` en el 5050, ambos `Up` → [`paso1_compose.txt`](evidencias/parte_a_50000/paso1_compose.txt)

pgAdmin: http://localhost:5050 (usuario `admin@paytrack.co`, clave `admin`). Para registrar el servidor
dentro de pgAdmin: host `postgres`, puerto `5432`, usuario y clave `postgres`.

## Paso 2 · Instalar dependencias y generar los datos

```bash
uv sync
uv run python labs/shared/generate_data.py --transactions 50000 --output data/
```

✅ 500 clientes, 8 comercios, 50.000 transacciones y 192 eventos de fraude → [`paso2_generate_data.txt`](evidencias/parte_a_50000/paso2_generate_data.txt)

`data/` está en `.gitignore`: cada quien genera sus datos. Las transacciones cubren los **últimos 90 días
contados desde hoy** y las consultas usan `CURRENT_DATE`, así que hay que generar, cargar y tomar
capturas **el mismo día**. Como los datos son aleatorios, los números de cada uno cambian un poco.

## Paso 3 · Cargar el OLTP

```bash
uv run python labs/01-modelado/load_oltp.py
```

✅ `¡Carga OLTP completada!` → [`paso3_load_oltp.txt`](evidencias/parte_a_50000/paso3_load_oltp.txt)

### Paso 3b · Verificar el OLTP (agregado por nosotros)

```bash
docker exec -i docker-postgres-1 psql -U postgres -d paytrack < labs/01-modelado/sql/verify_oltp.sql
```

[`sql/verify_oltp.sql`](sql/verify_oltp.sql) confirma que la carga está completa y es consistente
→ [`paso3b_verify_oltp.txt`](evidencias/parte_a_50000/paso3b_verify_oltp.txt)

| Verificación | 50K | 500K |
|---|---|---|
| customers / merchants / payment_methods | 500 / 8 / 5 | 500 / 8 / 5 |
| transactions | 50.000 | 500.000 |
| approved / declined | 49.931 / 69 | 499.418 / 582 |
| fraudes (= fraud_events) | 192 | 1.947 |
| Chequeos de consistencia (fraude sin evento, evento sin fraude, eventos duplicados, transacción antes del alta del cliente) | **todos 0** | **todos 0** |

## Paso 4 · Ejecutar `sql/oltp_problems.sql` y guardar cada `EXPLAIN ANALYZE`

```bash
docker exec -i docker-postgres-1 psql -U postgres -d paytrack -c "ANALYZE;"
docker exec -i docker-postgres-1 psql -U postgres -d paytrack -a < labs/01-modelado/sql/oltp_problems.sql
```

El script hace una pasada previa para calentar la caché y guarda la segunda, con el texto de cada
consulta antes de su plan:
- 50K → [`evidencias/parte_a_50000/paso4_explain_oltp.txt`](evidencias/parte_a_50000/paso4_explain_oltp.txt)
- 500K → [`evidencias/parte_a_500000/paso4_explain_oltp.txt`](evidencias/parte_a_500000/paso4_explain_oltp.txt)

| Consulta | 50K (569 págs.) | 500K (5.676 págs.) | Crecimiento con 10× datos |
|---|---|---|---|
| Q1 · monto por categoría (30 días) | 9,1 ms · Bitmap Index/Heap Scan + Hash Join | 56,5 ms · **cambia a Parallel Seq Scan** (2 workers) + Hash Join | 6,2× |
| Q2 · fraude por segmento | 18,6 ms · Seq Scan + Hash Join con 50.000 filas | 78,4 ms · Parallel Seq Scan + Hash Join con 500.000 filas | 4,2× |
| Q3 · volumen por día/hora (7 días) | 6,7 ms · Bitmap Scan + Sort por `EXTRACT()` (277 kB) | 51,7 ms · igual, Sort de 3,2 MB; estima 37.868 grupos y salen 168 | 7,8× |

## Paso 5 · Responder en el informe

Para cada consulta: qué operaciones dominan, por qué el 3FN obliga a ese trabajo y qué pasaría con
500K transacciones diarias durante un año → **redactado en [`informe/informe_lab1.md`](informe/informe_lab1.md), sección 1.**

## Checklist Parte A

- [x] 1. Postgres y pgAdmin arriba con el compose del repo
- [x] 2. Dependencias instaladas y datos generados
- [x] 3. OLTP cargado (+ verificado con `verify_oltp.sql`)
- [x] 4. `EXPLAIN ANALYZE` de Q1–Q3 guardado (50K y 500K)
- [x] 5. Respuestas redactadas en el borrador del informe
- [ ] Revisión del equipo del texto de la sección 1 del informe
