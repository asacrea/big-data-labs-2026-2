#!/usr/bin/env bash
# ============================================================
# Lab 1 · Parte A — El OLTP y sus límites
# Ejecuta los pasos 1 a 4 del README del lab, con los mismos comandos
# y en el mismo orden, y guarda la salida de cada paso en
# evidencias/parte_a_<N>/. El paso 5 (responder) está en el informe.
#
# Uso (desde la raíz del repo, en Git Bash):
#   bash labs/01-modelado/run_parte_a.sh            # 50.000 transacciones (README, paso 2)
#   bash labs/01-modelado/run_parte_a.sh 500000     # volumen que sugiere el README en la Parte D
#
# Archivo nuestro, no del profesor. No modifica ningún archivo del lab.
# ============================================================
set -euo pipefail

N=${1:-50000}
cd "$(dirname "$0")/../.."

# Ajustes de Windows (ver PARTE_A.md): consola UTF-8 y Postgres por IPv4
export PYTHONUTF8=1
export DATABASE_URL=${DATABASE_URL:-postgresql://postgres:postgres@127.0.0.1:5432/paytrack}

if [ "$N" = 50000 ]; then DATA=data; else DATA=data/$N; fi
OUT=labs/01-modelado/evidencias/parte_a_$N
rm -rf "$OUT" && mkdir -p "$OUT"
PSQL="docker exec -i docker-postgres-1 psql -U postgres -d paytrack"

echo "== 1. Levanta PostgreSQL y pgAdmin"
docker compose -f .docker/docker-compose.yml up -d postgres pgadmin 2>&1 | tee "$OUT/paso1_levantar_postgres_pgadmin.txt"
until docker exec docker-postgres-1 pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done
docker compose -f .docker/docker-compose.yml ps | tee -a "$OUT/paso1_levantar_postgres_pgadmin.txt"

echo "== 2. Instala las dependencias y genera los datos de prueba"
uv sync 2>&1 | tail -1 | tee "$OUT/paso2_dependencias_y_datos.txt"
uv run python labs/shared/generate_data.py --transactions "$N" --output "$DATA/" \
    | tee -a "$OUT/paso2_dependencias_y_datos.txt"

echo "== 3. Carga los datos en el esquema OLTP"
uv run python labs/01-modelado/load_oltp.py --data-dir "$DATA" | tee "$OUT/paso3_carga_oltp.txt"

echo "== 4. Ejecuta las consultas de sql/oltp_problems.sql y guarda la salida de cada EXPLAIN ANALYZE"
# ANALYZE actualiza las estadísticas de las tablas recién cargadas para que el
# optimizador planee con datos reales (si no, depende de cuándo pase autovacuum).
$PSQL -q -c "ANALYZE;"
# -a imprime el texto de cada consulta antes de su plan; luego se separa un archivo por consulta.
$PSQL -a < labs/01-modelado/sql/oltp_problems.sql > "$OUT/.paso4_completo.txt"
awk -v out="$OUT" '
    /^-- Q[123]:/ { n = substr($2, 2, 1); f = out "/paso4_q" n "_explain_analyze.txt" }
    f { print > f }
' "$OUT/.paso4_completo.txt"
rm "$OUT/.paso4_completo.txt"

echo "== Anexo · verificación de la carga (sql/verify_oltp.sql, nuestro)"
$PSQL < labs/01-modelado/sql/verify_oltp.sql > "$OUT/anexo_verificacion_oltp.txt"
grep -A6 debe_ser_0 "$OUT/anexo_verificacion_oltp.txt"

echo
echo "Evidencias en $OUT/"
grep -H "Execution Time" "$OUT"/paso4_q*_explain_analyze.txt
