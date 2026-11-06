# Lab 1 · Modelado OLTP → OLAP

**Entrega:** viernes 23 de octubre de 2026 · **Peso:** 15%

## Objetivo

Diseñar e implementar el esquema estrella (OLAP) de PayTrack a partir de su esquema transaccional (OLTP), cargarlo con un proceso ELT y demostrar con planes de ejecución por qué el modelo dimensional responde mejor las preguntas analíticas.

## Contexto del negocio

PayTrack Colombia procesa 500K transacciones diarias entre clientes, comercios y métodos de pago. El equipo de BI necesita responder preguntas como:

- ¿Cuál es el monto total procesado por categoría de comercio en los últimos 30 días?
- ¿Qué segmento de clientes tiene mayor tasa de fraude?
- ¿Cómo se distribuye el volumen de transacciones por día de la semana y hora?

El esquema OLTP actual fue diseñado para registrar pagos, no para analizarlos.

## Qué está dado y qué construyes tú

| Archivo | Estado | Qué hace |
|---------|--------|----------|
| `sql/oltp_schema.sql` | ✅ Dado | Esquema transaccional normalizado (3FN) |
| `load_oltp.py` | ✅ Dado | Carga los datos generados al OLTP |
| `sql/oltp_problems.sql` | ✅ Dado | Las 3 consultas de negocio sobre el OLTP |
| `sql/star_schema.sql` | ✏️ **TODO** | Diseño del esquema estrella (dim_date y dim_time están dadas) |
| `load_star.py` | ✏️ **TODO** | SQL de carga de dimensiones y tabla de hechos |
| `sql/olap_queries.sql` | ✏️ **TODO** | Las 3 consultas reescritas sobre el esquema estrella |
| `check_queries.py` | ✅ Dado | Autoevaluación de tus consultas |

## Parte A — El OLTP y sus límites

1. Levanta PostgreSQL y pgAdmin:
   ```bash
   docker compose -f .docker/docker-compose.yml up -d postgres pgadmin
   ```
   pgAdmin queda en http://localhost:5050 (usuario `admin@paytrack.co`, clave `admin`).

2. Instala las dependencias y genera los datos de prueba:
   ```bash
   uv sync
   uv run python labs/shared/generate_data.py --transactions 50000 --output data/
   ```

3. Carga los datos en el esquema OLTP:
   ```bash
   uv run python labs/01-modelado/load_oltp.py
   ```

4. Ejecuta las consultas de `sql/oltp_problems.sql` y guarda la salida de cada `EXPLAIN ANALYZE`.

5. Para cada consulta, responde en tu informe:
   - ¿Qué operaciones dominan el plan (scans, joins, sorts)?
   - ¿Por qué el diseño en 3FN obliga a hacer ese trabajo?
   - ¿Qué pasaría con estos tiempos con 500K transacciones diarias durante un año?

## Parte B — Diseño del esquema estrella

Antes de escribir SQL, decide y deja por escrito en el informe:

- **Proceso de negocio y grano** de la tabla de hechos.
- **Dimensiones** y los atributos de cada una. ¿Qué datos del OLTP *no* deben llegar al data warehouse?
- **Medidas** de la tabla de hechos y si son aditivas, semiaditivas o no aditivas.
- **Por qué surrogate keys** en lugar de las llaves del OLTP.
- **Por qué SCD Tipo 2** para la dimensión cliente: ¿qué atributo puede cambiar y qué análisis se daña si se sobrescribe?

Luego completa los TODO de `sql/star_schema.sql`. Respeta el contrato mínimo descrito al inicio del archivo: lo usan los scripts de validación.

## Parte C — Carga ELT

Completa los 4 TODO de `load_star.py` (cada uno es un `INSERT ... SELECT` de `oltp.*` a `dw.*`) y ejecútalo:

```bash
uv run python labs/01-modelado/load_star.py
```

El script recrea el esquema `dw` en cada ejecución y al final valida tu carga: conteos, montos, fraudes e integridad referencial. **Todas las validaciones deben pasar.**

## Parte D — Consultas OLAP y comparación

1. Completa `sql/olap_queries.sql`: las mismas 3 preguntas de negocio, usando solo tablas `dw.*`.

2. Verifica que tus resultados sean idénticos a los del OLTP:
   ```bash
   uv run python labs/01-modelado/check_queries.py
   ```

3. Ejecuta ambas versiones con `EXPLAIN ANALYZE` y compara: tiempo total, tipos de scan y joins, filas procesadas.

4. Experimenta con los índices de la tabla de hechos (TODO 5 de `star_schema.sql`): mide el plan con y sin cada índice. ¿Todos ayudan? ¿Por qué el optimizador a veces prefiere un *sequential scan*?

> 💡 Con 50K filas las diferencias pueden ser pequeñas. Prueba también con `--transactions 500000` y compara cómo crecen los tiempos en cada modelo.

## Entregables

- [ ] Tu `sql/star_schema.sql`, `load_star.py` y `sql/olap_queries.sql` en tu repositorio
- [ ] Captura de `load_star.py` con todas las validaciones en verde
- [ ] Captura de `check_queries.py` con 3/3 consultas correctas
- [ ] Diagrama del esquema estrella hecho en [draw.io](https://app.diagrams.net/): tablas, llaves, cardinalidades y grano de la tabla de hechos
- [ ] Informe en PDF (máx. 3 páginas): diagnóstico del OLTP (Parte A), decisiones de diseño (Parte B) y comparación de rendimiento con planes de ejecución (Parte D)

## Criterios de evaluación

| Criterio | Peso |
|----------|------|
| Diseño del esquema estrella: grano, dimensiones, medidas, SCD2, justificación | 35% |
| Carga ELT correcta (validaciones de `load_star.py` en verde) | 20% |
| Consultas OLAP correctas (`check_queries.py` 3/3) | 15% |
| Análisis de rendimiento OLTP vs OLAP e índices | 20% |
| Calidad del informe y del diagrama | 10% |
