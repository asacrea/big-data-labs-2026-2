"""
Productor del ejercicio de particionamiento: pagos de PayTrack de 4 ciudades.

Uso:
    python clases/kafka-particiones/productor.py --modo ciudad      # llave = ciudad
    python clases/kafka-particiones/productor.py --modo sin-llave   # sin llave
    python clases/kafka-particiones/productor.py --modo manual      # cada ciudad a una partición fija

Cada pago lleva un número de secuencia por ciudad (seq): 1, 2, 3... dentro de
cada corrida del productor (run). Si el consumidor recibe los pagos de una
ciudad en otro orden, lo detecta.
"""

import argparse
import json
import random
import time
import uuid

from confluent_kafka import Producer

# Peso de cada ciudad en el tráfico: Bogotá recibe casi la mitad de los pagos
CIUDADES = {"Bogotá": 45, "Medellín": 30, "Cali": 15, "Barranquilla": 10}
# Modo manual: la partición que le asignamos a cada ciudad
PARTICION_MANUAL = {"Bogotá": 0, "Medellín": 1, "Cali": 2, "Barranquilla": 3}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--modo", choices=["ciudad", "sin-llave", "manual"], default="ciudad")
    parser.add_argument("--topic", default="pagos-ciudades")
    parser.add_argument("--tasa", type=int, default=10, help="pagos por segundo")
    parser.add_argument("--segundos", type=int, default=60)
    parser.add_argument("--bootstrap", default="localhost:9092")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap, "broker.address.family": "v4"})
    try:
        producer.list_topics(timeout=10)
    except Exception:
        raise SystemExit(f"✗ No hay conexión con Kafka en {args.bootstrap}. ¿Está corriendo? "
                         "docker compose -f .docker/docker-compose.yml up -d kafka")
    seq = dict.fromkeys(CIUDADES, 0)
    run = uuid.uuid4().hex[:6]
    enviados: dict[tuple[str, int], int] = {}

    def entregado(err, msg):
        if err is None:
            ciudad = json.loads(msg.value())["ciudad"]
            enviados[(ciudad, msg.partition())] = enviados.get((ciudad, msg.partition()), 0) + 1

    print(f"Enviando ~{args.tasa} pagos/s a '{args.topic}' durante {args.segundos}s en modo '{args.modo}' "
          "(Ctrl+C para parar)")
    try:
        for _ in range(args.segundos * args.tasa):
            ciudad = random.choices(list(CIUDADES), weights=list(CIUDADES.values()))[0]
            seq[ciudad] += 1
            pago = {"event_id": str(uuid.uuid4()), "run": run, "ciudad": ciudad, "seq": seq[ciudad],
                    "customer_id": f"C{random.randint(1, 500):06d}",
                    "amount_cop": round(random.lognormvariate(10.5, 1.0), 2)}
            kwargs = {"value": json.dumps(pago, ensure_ascii=False), "on_delivery": entregado}
            if args.modo == "ciudad":
                kwargs["key"] = ciudad
            elif args.modo == "manual":
                kwargs["partition"] = PARTICION_MANUAL[ciudad]
            producer.produce(args.topic, **kwargs)
            producer.poll(0)
            time.sleep(1 / args.tasa)
    except KeyboardInterrupt:
        pass
    producer.flush(10)

    print("\nPagos enviados por ciudad y partición:")
    for (ciudad, particion), n in sorted(enviados.items()):
        print(f"  {ciudad:13s} → partición {particion}: {n}")


if __name__ == "__main__":
    main()
