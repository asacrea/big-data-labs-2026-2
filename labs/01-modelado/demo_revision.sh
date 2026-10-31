#!/usr/bin/env bash
# ============================================================
# Demostración de la revisión de la Parte B (PR #1)
#
# Muestra, paso a paso y en la máquina de quien lo ejecuta:
#   1. la versión de main (GitHub) fallando en Postgres,
#   2. la rama correcciones-parte-b pasando la autoevaluación.
#
# Uso, desde la raíz del repo (Git Bash en Windows, o terminal en Mac/Linux):
#   git fetch origin
#   git show origin/correcciones-parte-b:labs/01-modelado/demo_revision.sh > /tmp/demo.sh && bash /tmp/demo.sh
#
# No cambia de rama ni modifica tu copia: cada versión se ejecuta en una
# carpeta temporal (git worktree) que se borra al final.
# Requisitos: Docker con el contenedor postgres del repo y uv.
# ============================================================
set -uo pipefail

export PYTHONUTF8=1
export DATABASE_URL=${DATABASE_URL:-postgresql://postgres:postgres@127.0.0.1:5432/paytrack}
REPO=$(git rev-parse --show-toplevel)
TMP=$(mktemp -d)
PSQL="docker exec -i docker-postgres-1 psql -U postgres -d paytrack"
B=$'\e[1m'; C=$'\e[36m'; Y=$'\e[33m'; G=$'\e[32m'; R=$'\e[31m'; N=$'\e[0m'

paso()  { echo; echo "${C}${B}══ $* ══${N}"; }
cmd()   { echo "${Y}\$ $*${N}"; }
pausa() { echo; read -r -p "${B}[Enter para continuar]${N} " _; }
limpiar() {
    cd "$REPO" >/dev/null 2>&1
    git worktree remove --force "$TMP/main" >/dev/null 2>&1
    git worktree remove --force "$TMP/rama" >/dev/null 2>&1
    rm -rf "$TMP"
}
trap limpiar EXIT

paso "0. Preparación"
cd "$REPO"
git fetch -q origin
docker compose -f .docker/docker-compose.yml up -d postgres >/dev/null 2>&1
until docker exec docker-postgres-1 pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done
git worktree add -q --detach "$TMP/main" origin/main
git worktree add -q --detach "$TMP/rama" origin/correcciones-parte-b
echo "Postgres arriba. Versiones a comparar:"
echo "  main                 → $(git log -1 --format='%h %an %ad %s' --date=short origin/main)"
echo "  correcciones-parte-b → $(git log -1 --format='%h %an %ad %s' --date=short origin/correcciones-parte-b)"
if [ "$($PSQL -tAc 'SELECT COUNT(*) FROM oltp.transactions' 2>/dev/null | tr -d '[:space:]')" = "" ] ||
   [ "$($PSQL -tAc 'SELECT COUNT(*) FROM oltp.transactions' 2>/dev/null | tr -d '[:space:]')" = "0" ]; then
    echo "El OLTP está vacío: se generan y cargan los datos del enunciado (Parte A)…"
    uv run python labs/shared/generate_data.py --transactions 50000 --output data/ >/dev/null
    uv run python labs/01-modelado/load_oltp.py >/dev/null
fi
echo "OLTP: $($PSQL -tAc 'SELECT COUNT(*) FROM oltp.transactions' | tr -d '[:space:]') transacciones"
pausa

# ── Versión de main ──────────────────────────────────────────────────────────
cd "$TMP/main"

paso "1. main · star_schema.sql: crear el esquema dw como lo hace load_star.py"
cmd "psql < star_schema.sql   (versión de main)"
$PSQL -q -c "SET client_min_messages = warning; DROP SCHEMA IF EXISTS dw CASCADE"
$PSQL -q < labs/01-modelado/sql/star_schema.sql 2>&1 | grep -E "ERROR|LINE" | sed "s/^/${R}/;s/$/${N}/"
pausa

paso "2. main · tipos de dato frente a los datos reales del OLTP"
cmd "transaction_id es INT en main; en el OLTP es:"
$PSQL -tAc "SELECT transaction_id || '  (' || pg_typeof(transaction_id) || ')' FROM oltp.transactions LIMIT 1"
$PSQL -tAc "SELECT transaction_id::INT FROM oltp.transactions LIMIT 1" 2>&1 | sed "s/^/${R}/;s/$/${N}/"
cmd "fraud_risk_score es DECIMAL(5,2) en main; el OLTP tiene 4 decimales:"
$PSQL -P footer=off -c "SELECT risk_score AS oltp, risk_score::DECIMAL(5,2) AS main FROM oltp.fraud_events LIMIT 3"
cmd "income_band es DECIMAL(18,2) en main; un rango es texto:"
$PSQL -tAc "SELECT '3-6M'::DECIMAL(18,2)" 2>&1 | head -1 | sed "s/^/${R}/;s/$/${N}/"
pausa

paso "3. main · load_star.py tal cual"
cmd "uv run python labs/01-modelado/load_star.py   (versión de main)"
uv run --project "$REPO" python labs/01-modelado/load_star.py 2>&1 | tail -4 | sed "s/^/${R}/;s/$/${N}/"
pausa

paso "4. main · las 4 consultas de carga de load_star.py, una por una (sobre el esquema corregido)"
uv run --project "$REPO" python - "$TMP/main/labs/01-modelado/load_star.py" "$TMP/rama/labs/01-modelado/sql/star_schema.sql" <<'PY'
import importlib.util, sys, psycopg2
spec = importlib.util.spec_from_file_location("ls_main", sys.argv[1])
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
conn = psycopg2.connect(m.DB_URL); cur = conn.cursor()
cur.execute("DROP SCHEMA IF EXISTS dw CASCADE"); cur.execute(open(sys.argv[2], encoding="utf-8").read())
cur.execute(m.SQL_DIM_DATE); cur.execute(m.SQL_DIM_TIME); conn.commit()
for name, sql in [("dim_customer", m.SQL_DIM_CUSTOMER), ("dim_merchant", m.SQL_DIM_MERCHANT),
                  ("dim_payment_method", m.SQL_DIM_PAYMENT_METHOD), ("fact_transactions", m.SQL_FACT)]:
    try:
        cur.execute(sql); conn.commit(); print(f"  \033[32m✓ {name}: {cur.rowcount} filas\033[0m")
    except psycopg2.Error as e:
        conn.rollback(); print(f"  \033[31m✗ {name}: {str(e).strip().splitlines()[0]}\033[0m")
cur.execute("SELECT COUNT(*) FROM dw.dim_payment_method WHERE method_type IS NOT NULL")
n = cur.fetchone()[0]
mark = "\033[32m✓" if n == 5 else "\033[31m✗"
print(f"  {mark} dim_payment_method con method_type: {n} de 5\033[0m")
PY
pausa

paso "5. main · olap_queries.sql"
cmd "psql < olap_queries.sql   (versión de main)"
$PSQL -q < labs/01-modelado/sql/olap_queries.sql 2>&1 | grep -E "ERROR|HINT" | sed "s/^/${R}/;s/$/${N}/"
cmd "columnas que devuelve la versión OLTP (las exige check_queries.py) frente a main:"
echo "  OLTP Q2: segment, total_txn, fraud_txn, fraud_rate_pct   | main Q2: …, $(grep -o 'AS tasa_fraude' labs/01-modelado/sql/olap_queries.sql | head -1)"
echo "  OLTP Q3: day_of_week, hour, num_transactions, total_cop | main Q3: $(grep -oE 'AS (dia_semana|hora|num_txn)' labs/01-modelado/sql/olap_queries.sql | tr '\n' ' ')"
pausa

# ── Rama con las correcciones ────────────────────────────────────────────────
cd "$TMP/rama"

paso "6. correcciones-parte-b · load_star.py"
cmd "uv run python labs/01-modelado/load_star.py   (versión de la rama)"
uv run --project "$REPO" python labs/01-modelado/load_star.py
pausa

paso "7. correcciones-parte-b · check_queries.py"
cmd "uv run python labs/01-modelado/check_queries.py   (versión de la rama)"
uv run --project "$REPO" python labs/01-modelado/check_queries.py
cmd "precisión de fraud_risk_score frente al OLTP:"
$PSQL -P footer=off -c "SELECT COUNT(fraud_risk_score) AS con_score, (SELECT COUNT(*) FROM oltp.fraud_events) AS eventos_oltp, SUM(fraud_risk_score) AS suma_dw, (SELECT SUM(risk_score) FROM oltp.fraud_events) AS suma_oltp FROM dw.fact_transactions"

paso "Fin"
echo "Tu copia y tu rama actual no cambiaron. Las carpetas temporales se borran al salir."
echo "El esquema dw de tu Postgres queda cargado con la versión de la rama."
