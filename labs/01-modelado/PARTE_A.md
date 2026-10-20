# Parte A — El OLTP y sus límites

Ejecución de la Parte A del [README del lab](README.md), **en el mismo orden y con los mismos comandos**.
Bajo cada paso, en cita, va el texto del enunciado tal cual; debajo, la evidencia de que se ejecutó.
Las respuestas del paso 5 están en el [informe](informe/informe_lab1.md), sección *Diagnóstico del OLTP (Parte A)*.

**Estado:** ✅ pasos 1 a 5 completos (19-oct-2026).

Para repetir los pasos 1 a 4 de una vez y regenerar todas las evidencias:

```bash
bash labs/01-modelado/run_parte_a.sh
```

---

## 1. Levanta PostgreSQL y pgAdmin

> ```bash
> docker compose -f .docker/docker-compose.yml up -d postgres pgadmin
> ```
> pgAdmin queda en http://localhost:5050 (usuario `admin@paytrack.co`, clave `admin`).

**Evidencia:** [`evidencias/parte_a_50000/paso1_levantar_postgres_pgadmin.txt`](evidencias/parte_a_50000/paso1_levantar_postgres_pgadmin.txt).
Los contenedores `postgres` (puerto 5432) y `pgadmin` (puerto 5050) quedan `Up`.

## 2. Instala las dependencias y genera los datos de prueba

> ```bash
> uv sync
> uv run python labs/shared/generate_data.py --transactions 50000 --output data/
> ```

**Evidencia:** [`evidencias/parte_a_50000/paso2_dependencias_y_datos.txt`](evidencias/parte_a_50000/paso2_dependencias_y_datos.txt).
Se generaron 500 clientes, 8 comercios, 50.000 transacciones y 200 eventos de fraude.

## 3. Carga los datos en el esquema OLTP

> ```bash
> uv run python labs/01-modelado/load_oltp.py
> ```

**Evidencia:** [`evidencias/parte_a_50000/paso3_carga_oltp.txt`](evidencias/parte_a_50000/paso3_carga_oltp.txt).
El script termina con `¡Carga OLTP completada!`.

## 4. Ejecuta las consultas de `sql/oltp_problems.sql` y guarda la salida de cada `EXPLAIN ANALYZE`

**Evidencia:** un archivo por consulta, con el texto de la consulta seguido de su plan.

| Consulta (título tal cual en `oltp_problems.sql`) | Salida del `EXPLAIN ANALYZE` | Execution Time |
|---|---|---|
| Q1: Monto total por categoría de comercio (últimos 30 días) | [`paso4_q1_explain_analyze.txt`](evidencias/parte_a_50000/paso4_q1_explain_analyze.txt) | 9,586 ms |
| Q2: Tasa de fraude por segmento de cliente | [`paso4_q2_explain_analyze.txt`](evidencias/parte_a_50000/paso4_q2_explain_analyze.txt) | 17,562 ms |
| Q3: Volumen de transacciones por día de la semana y hora (últimos 7 días) | [`paso4_q3_explain_analyze.txt`](evidencias/parte_a_50000/paso4_q3_explain_analyze.txt) | 4,864 ms |

Antes de las consultas se ejecutó `ANALYZE;` para que el optimizador planee con las estadísticas de las tablas recién cargadas.

## 5. Para cada consulta, responde en tu informe

> - ¿Qué operaciones dominan el plan (scans, joins, sorts)?
> - ¿Por qué el diseño en 3FN obliga a hacer ese trabajo?
> - ¿Qué pasaría con estos tiempos con 500K transacciones diarias durante un año?

**Respondido** en el [informe](informe/informe_lab1.md), sección 1: las tres preguntas para cada una de Q1, Q2 y Q3, con las mismas palabras del enunciado.

---

## Anexos (complementos; no reemplazan ningún paso)

### A. Medición con 500.000 transacciones

El README (Parte D) sugiere: *"Prueba también con `--transactions 500000` y compara cómo crecen los tiempos en cada modelo"*.
Se repitieron los pasos 1 a 4 con ese volumen para sustentar con datos la tercera pregunta del paso 5:

```bash
bash labs/01-modelado/run_parte_a.sh 500000   # luego volver a 50K: bash labs/01-modelado/run_parte_a.sh
```

| Consulta | 50.000 | 500.000 | Evidencia 500K |
|---|---|---|---|
| Q1 | 9,586 ms | 52,036 ms | [`paso4_q1_explain_analyze.txt`](evidencias/parte_a_500000/paso4_q1_explain_analyze.txt) |
| Q2 | 17,562 ms | 80,863 ms | [`paso4_q2_explain_analyze.txt`](evidencias/parte_a_500000/paso4_q2_explain_analyze.txt) |
| Q3 | 4,864 ms | 50,865 ms | [`paso4_q3_explain_analyze.txt`](evidencias/parte_a_500000/paso4_q3_explain_analyze.txt) |

La base quedó cargada otra vez con 50.000, el volumen del enunciado, para las Partes B a D.

### B. Verificación de la carga

[`sql/verify_oltp.sql`](sql/verify_oltp.sql) comprueba conteos, rango de fechas y consistencia: ninguna transacción
fraudulenta sin evento, ningún evento sobre una transacción no fraudulenta, ningún evento duplicado y ninguna
transacción anterior al alta del cliente. Los cuatro chequeos dan 0 en los dos volúmenes
([50K](evidencias/parte_a_50000/anexo_verificacion_oltp.txt) · [500K](evidencias/parte_a_500000/anexo_verificacion_oltp.txt)).

### C. Ajustes para Windows (no modifican ningún archivo del profesor)

1. `PYTHONUTF8=1`: la consola de Windows no imprime el carácter `✓` de los scripts (`UnicodeEncodeError`).
2. `DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/paytrack`: WSL ocupa `[::1]:5432` y `localhost`
   resuelve a IPv6. Los scripts del lab leen esta variable, así que basta con forzar IPv4.

`run_parte_a.sh` ya los aplica. Para correr los comandos a mano en Git Bash:

```bash
export PYTHONUTF8=1
export DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/paytrack
```

### D. Notas

- Las transacciones cubren los últimos 90 días contados desde la fecha de generación, y las consultas usan
  `CURRENT_DATE`. Hay que generar, cargar y capturar el mismo día.
- Los datos son aleatorios: cada integrante obtendrá conteos y tiempos un poco distintos.
- Para registrar el servidor en pgAdmin: host `postgres`, puerto `5432`, usuario y clave `postgres`.
