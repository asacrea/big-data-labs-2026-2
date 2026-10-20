#!/usr/bin/env bash
# ============================================================
# Lab 1 · Parte A — ejecuta los pasos 1 a 4 del README tal cual
# y guarda la salida de cada uno en evidencias/parte_a_<N>/.
#
# Uso (desde la raíz del repo, en Git Bash):
#   bash labs/01-modelado/run_parte_a.sh            # 50.000 transacciones (README)
#   bash labs/01-modelado/run_parte_a.sh 500000     # la prueba de escala que sugiere el README
#
# Archivo nuestro, no del profesor. No modifica ningún script del lab.
# ============================================================
set -euo pipefail

N=${1:-50000}
cd "$(dirname "$0")/../.."

# Ajustes de Windows (ver PARTE_A.md): consola UTF-8 y Postgres por IPv4
export PYTHONUTF8=1
export DATABASE_URL=${DATABASE_URL:-postgresql://postgres:postgres@127.0.0.1:5432/paytrack}

if [ "$N" = 50000 ]; then DATA=data; else DATA=data/$N; fi
OUT=labs/01-modelado/evidencias/parte_a_$N
mkdir -p "$OUT"
PSQL="docker exec -i docker-postgres-1 psql -U postgres -d paytrack"

echo "== Paso 1 · Levantar PostgreSQL y pgAdmin"
docker compose -f .docker/docker-compose.yml up -d postgres pgadmin 2>&1 | tee "$OUT/paso1_compose.txt"
until docker exec docker-postgres-1 pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done
docker compose -f .docker/docker-compose.yml ps | tee -a "$OUT/paso1_compose.txt"

echo "== Paso 2 · Instalar dependencias y generar datos ($N transacciones)"
uv sync 2>&1 | tail -1 | tee "$OUT/paso2_generate_data.txt"
uv run python labs/shared/generate_data.py --transactions "$N" --output "$DATA/" \
    | tee -a "$OUT/paso2_generate_data.txt"

echo "== Paso 3 · Cargar el OLTP"
uv run python labs/01-modelado/load_oltp.py --data-dir "$DATA" | tee "$OUT/paso3_load_oltp.txt"

echo "== Paso 3b · Verificar el OLTP (sql/verify_oltp.sql)"
$PSQL < labs/01-modelado/sql/verify_oltp.sql | tee "$OUT/paso3b_verify_oltp.txt"

echo "== Paso 4 · EXPLAIN ANALYZE de sql/oltp_problems.sql"
# ANALYZE actualiza estadísticas; la primera pasada calienta la caché y se descarta,
# la segunda es la que se guarda (con el texto de cada consulta antes de su plan).
$PSQL -q -c "ANALYZE;"
$PSQL -q < labs/01-modelado/sql/oltp_problems.sql > /dev/null
$PSQL -a < labs/01-modelado/sql/oltp_problems.sql | tee "$OUT/paso4_explain_oltp.txt"

echo
echo "Evidencias en $OUT/"
grep -E "Execution Time" "$OUT/paso4_explain_oltp.txt"
