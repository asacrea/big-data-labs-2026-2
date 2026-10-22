#!/usr/bin/env bash
# ============================================================
# Lab 1 · Parte D — Comparación de rendimiento
# Ejecuta los pasos 1 a 4 del README del lab, en el mismo orden, y guarda la
# salida de cada paso en evidencias/parte_d_<N>/.
#
# Uso (desde la raíz del repo, en Git Bash):
#   bash labs/01-modelado/run_parte_d.sh            # 50.000 transacciones
#   bash labs/01-modelado/run_parte_d.sh 500000     # 💡 del README: "Prueba también con --transactions 500000"
#
# Archivo nuestro, no del profesor. No modifica ningún archivo del lab: los
# índices del paso 4 se quitan dentro de una transacción que se revierte.
# ============================================================
set -euo pipefail

N=${1:-50000}
cd "$(dirname "$0")/../.."

# Ajustes de Windows (ver PARTE_A.md): consola UTF-8, Postgres por IPv4 y docker en el PATH
# ANCHO_EVIDENCIA y no COLUMNS: bash actualiza COLUMNS al ancho real de la consola tras cada comando
export PYTHONUTF8=1 FORCE_COLOR=1 ANCHO_EVIDENCIA=140
export DATABASE_URL=${DATABASE_URL:-postgresql://postgres:postgres@127.0.0.1:5432/paytrack}
export PATH="$PATH:/c/Program Files/Docker/Docker/resources/bin"

if [ "$N" = 50000 ]; then DATA=data; else DATA=data/$N; fi
OUT=labs/01-modelado/evidencias/parte_d_$N
rm -rf "$OUT" && mkdir -p "$OUT"
PSQL="docker exec -i docker-postgres-1 psql -U postgres -d paytrack"
B=$'\e[1m'; C=$'\e[36m'; N0=$'\e[0m'
paso() { echo; echo "${C}${B}══ $1 ══${N0}"; shift; for l in "$@"; do echo "   > $l"; done; echo; }
# Muestra la salida en la consola (con color) y la guarda sin códigos de color
guardar() { tee >(sed 's/\x1b\[[0-9;]*m//g' > "$1"); }

paso "Preparación: OLTP y esquema estrella con $N transacciones"
until docker exec docker-postgres-1 pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done
if [ "$($PSQL -tAc 'SELECT COUNT(*) FROM oltp.transactions' | tr -d '[:space:]')" != "$N" ]; then
    [ -f "$DATA/transactions.json" ] || uv run python labs/shared/generate_data.py --transactions "$N" --output "$DATA/" >/dev/null
    uv run python labs/01-modelado/load_oltp.py --data-dir "$DATA" | tail -1
fi
uv run python labs/01-modelado/load_star.py | guardar "$OUT/preparacion_load_star.txt" | tail -3
# Estadísticas frescas para que el optimizador planee con datos reales (igual que en la Parte A)
$PSQL -q -c "ANALYZE;"

paso "1. Completa sql/olap_queries.sql" \
     "Completa \`sql/olap_queries.sql\`: las mismas 3 preguntas de negocio, usando solo tablas \`dw.*\`."
{
    echo "Tablas que usa olap_queries.sql:"
    # Se cuenta sobre el SQL sin comentarios (el encabezado del profesor menciona EXTRACT())
    SQL=$(sed 's/--.*//' labs/01-modelado/sql/olap_queries.sql)
    grep -oE '\b(dw|oltp)\.[a-z_]+' <<< "$SQL" | sort | uniq -c
    echo "Referencias a oltp.*:              $(grep -cE '\boltp\.' <<< "$SQL" || true)  (debe ser 0)"
    echo "EXTRACT() / funciones de fecha:    $(grep -ciE 'EXTRACT|DATE_TRUNC|TO_CHAR' <<< "$SQL" || true)  (debe ser 0)"
    echo "Consultas con EXPLAIN ANALYZE:     $(grep -ciE '^EXPLAIN ANALYZE' <<< "$SQL")  (deben ser 3)"
} | guardar "$OUT/paso1_olap_queries_solo_dw.txt"

paso "2. Verifica que tus resultados sean idénticos a los del OLTP" \
     "uv run python labs/01-modelado/check_queries.py"
uv run python labs/01-modelado/check_queries.py | guardar "$OUT/paso2_check_queries.txt"
echo
echo "${B}Anexo del paso 2 · verificación estricta: celda por celda, sin redondeo, con sumas de control${N0}"
uv run python labs/01-modelado/verify_exactitud.py | guardar "$OUT/paso2_anexo_exactitud.txt"

paso "3 y 4. EXPLAIN ANALYZE de ambas versiones e índices de la tabla de hechos" \
     "3. Ejecuta ambas versiones con \`EXPLAIN ANALYZE\` y compara: tiempo total, tipos de scan y joins, filas procesadas." \
     "4. Experimenta con los índices de la tabla de hechos (TODO 5 de \`star_schema.sql\`): mide el plan con y sin cada índice." \
     "   ¿Todos ayudan? ¿Por qué el optimizador a veces prefiere un sequential scan?"
uv run python labs/01-modelado/parte_d_planes.py "$OUT"

echo
echo "Evidencias en $OUT/"
ls -1 "$OUT"
