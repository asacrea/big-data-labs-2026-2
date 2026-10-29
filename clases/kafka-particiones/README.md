# Ejercicio en clase · Particionamiento de topics en Kafka

**Duración:** ~60 minutos · **En parejas** · No se entrega

## Objetivo

Ver con tus propios ojos cómo la **llave** de un mensaje decide su **partición**, y cómo la partición decide dos cosas:

1. **El orden:** Kafka solo garantiza el orden *dentro* de una partición.
2. **El reparto del trabajo:** en un *consumer group*, cada partición la lee un solo consumidor.

El caso: PayTrack recibe pagos de 4 ciudades con tráfico desigual (Bogotá 45%, Medellín 30%, Cali 15%, Barranquilla 10%). Queremos un tablero por ciudad que vea los pagos de cada ciudad **en orden**.

## Preparación (5 min)

Necesitas lo mismo que para el Lab 2: Docker Desktop abierto (en Windows, trabaja dentro de WSL 2), Python ≥ 3.11 y `uv`.

**Todos los comandos se corren desde la raíz del repositorio**, en todas las terminales.

```bash
# 1. Trae la versión más reciente del repositorio (incluye esta carpeta y el servicio de Kafka)
git pull

# 2. Instala las dependencias de Python (incluye el cliente de Kafka)
uv sync --extra dev

# 3. Levanta Kafka
docker compose -f .docker/docker-compose.yml up -d kafka
```

Define este atajo **en cada terminal que abras** (funciona en bash y zsh):

```bash
kt() { docker compose -f .docker/docker-compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "$@"; }
```

Verifica que Kafka responde (puede tardar unos 15 segundos después de levantarlo):

```bash
kt --list
```

Si no muestra error, estás listo. La lista puede salir vacía.

Vas a necesitar **hasta 3 terminales** abiertas a la vez: una para el productor y una o dos para los consumidores.

Los dos programas del ejercicio:

- `productor.py --modo <ciudad | sin-llave | manual>`: envía pagos. Cada pago lleva un número de secuencia por ciudad (`seq`) y, al terminar, muestra a qué partición fue cada ciudad.
- `consumidor.py --grupo <nombre> --nombre <A, B, ...>`: muestra cada pago con su partición y marca `⚠ fuera de orden` si una ciudad llega desordenada. Con **Ctrl+C** se detiene e imprime un resumen por ciudad y partición.

## Paso 1 · Crear el topic (5 min)

```bash
kt --create --if-not-exists --topic pagos-ciudades --partitions 4
kt --describe --topic pagos-ciudades
```

## Paso 2 · La ciudad como llave (10 min)

**Terminal 1:** un consumidor.

```bash
uv run python clases/kafka-particiones/consumidor.py --grupo tablero --nombre A
```

**Terminal 2:** pagos con llave = ciudad, durante 30 segundos.

```bash
uv run python clases/kafka-particiones/productor.py --modo ciudad --segundos 30
```

Detén el consumidor con Ctrl+C y mira el resumen.

> **Para responder:**
> - ¿Cada ciudad quedó en una sola partición? ¿Hay alguna partición con dos ciudades? ¿Alguna vacía?
> - ¿Por qué pasa eso si tenemos 4 ciudades y 4 particiones? (pista: `partición = hash(llave) % número de particiones`)
> - ¿Hubo pagos fuera de orden?

## Paso 3 · Varios consumidores en el mismo grupo (15 min)

Abre **dos** consumidores en el **mismo grupo**, cada uno en su terminal:

```bash
uv run python clases/kafka-particiones/consumidor.py --grupo tablero2 --nombre A
uv run python clases/kafka-particiones/consumidor.py --grupo tablero2 --nombre B
```

Fíjate en la línea amarilla `» me asignaron las particiones ...` de cada uno. Luego envía pagos:

```bash
uv run python clases/kafka-particiones/productor.py --modo ciudad --segundos 30
```

Después:

1. Mientras el productor corre, **detén uno de los consumidores**. ¿Qué pasa con sus particiones en el otro?
2. Repite con **4 consumidores** y luego con **5**: abre más terminales con el mismo `--grupo tablero2` y nombres distintos (`--nombre C`, `--nombre D`, ...), define `kt` en cada una si la vas a usar, y vuelve a correr el productor.

> **Para responder:**
> - ¿Cuántas ciudades le tocaron a cada consumidor? ¿Quedó la carga equilibrada?
> - Con 4 consumidores, ¿alguno se quedó sin trabajo aunque tenga una partición? ¿Y con 5?
> - ¿Qué limita el número de consumidores útiles en un grupo?

## Paso 4 · Sin llave (10 min)

Usa un topic nuevo para no mezclar resultados:

```bash
kt --create --if-not-exists --topic pagos-sin-llave --partitions 4
uv run python clases/kafka-particiones/productor.py --modo sin-llave --topic pagos-sin-llave --tasa 50 --segundos 10
```

Ahora, **después** de que terminó el productor, lee el historial con un grupo nuevo (empieza desde el primer mensaje):

```bash
uv run python clases/kafka-particiones/consumidor.py --grupo historico --topic pagos-sin-llave
```

> **Para responder:**
> - ¿En cuántas particiones quedó cada ciudad? ¿Cuántos pagos llegaron fuera de orden?
> - ¿Por qué ahora sí hay desorden? ¿Qué ganas y qué pierdes al no usar llave?

## Paso 5 · Asignación manual (5 min)

```bash
kt --create --if-not-exists --topic pagos-manual --partitions 4
uv run python clases/kafka-particiones/productor.py --modo manual --topic pagos-manual --segundos 10
```

> **Para responder:**
> - Ahora sí queda una ciudad por partición. ¿Qué pasa si PayTrack abre en una quinta ciudad?
> - Bogotá recibe el 45% del tráfico. ¿Qué consumidor se convierte en cuello de botella? ¿Cómo lo resolverías? (pista: ¿la llave tiene que ser solo la ciudad?)

## Paso 6 · Agregar particiones a un topic que ya existe (5 min)

```bash
kt --alter --topic pagos-ciudades --partitions 6
uv run python clases/kafka-particiones/productor.py --modo ciudad --segundos 10
```

Compara a qué partición va cada ciudad ahora con lo que viste en el paso 2.

> **Para responder:**
> - ¿Qué ciudades cambiaron de partición? ¿Qué le pasa al orden de esas ciudades justo en el cambio?
> - ¿Por qué Kafka permite **agregar** particiones pero no **quitarlas**?
> - Con 6 particiones, ¿cuántas reciben datos? ¿Sirvió de algo agregarlas?

## Cierre

| En Kafka | En Kinesis (Lab 2) |
|----------|--------------------|
| Topic | Stream |
| Partición | Shard |
| Llave del mensaje | Partition key |
| Offset | Sequence number |
| Consumer group | Aplicación consumidora (p. ej. el job de Glue) |

## Problemas frecuentes

| Síntoma | Solución |
|---------|----------|
| `✗ No hay conexión con Kafka en localhost:9092` | Kafka no está corriendo o aún está arrancando. Corre `docker compose -f .docker/docker-compose.yml up -d kafka`, espera unos segundos y prueba `kt --list`. |
| `kt: command not found` | Definiste `kt` en otra terminal. Vuelve a definirlo en esta. |
| `no such service: kafka` o `kt` no encuentra el contenedor | No tienes la última versión del repo (`git pull`) o no estás en la raíz del repositorio. |
| `ModuleNotFoundError: No module named 'confluent_kafka'` | Corre `uv sync --extra dev` y usa `uv run python ...`. |
| El consumidor no muestra nada | Revisa que el productor y el consumidor usen el mismo `--topic`. Un grupo que ya leyó todo solo recibe pagos nuevos: usa otro `--grupo` para leer desde el principio. |
| `port is already allocated` al levantar Kafka | Otro programa usa el puerto 9092. Detén ese otro Kafka (`docker ps` para encontrarlo). |

## Limpieza

```bash
kt --delete --topic pagos-ciudades
kt --delete --topic pagos-sin-llave
kt --delete --topic pagos-manual
docker compose -f .docker/docker-compose.yml stop kafka   # opcional: apagar Kafka
```
