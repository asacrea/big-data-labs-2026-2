"""
Consumidor del ejercicio de particionamiento.

Uso:
    python clases/kafka-particiones/consumidor.py --grupo tablero
    python clases/kafka-particiones/consumidor.py --grupo tablero --nombre B   # otra terminal, mismo grupo

Muestra cada pago con su partición. Al parar (Ctrl+C) imprime cuántos pagos
recibió de cada ciudad por partición y cuántos llegaron fuera de orden.
"""

import argparse
import json
import signal

from confluent_kafka import Consumer
from rich.console import Console
from rich.table import Table

console = Console()
COLOR = {"Bogotá": "red", "Medellín": "green", "Cali": "blue", "Barranquilla": "magenta"}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--grupo", required=True, help="consumer group")
    parser.add_argument("--nombre", default="A", help="nombre de este consumidor (para identificarlo)")
    parser.add_argument("--topic", default="pagos-ciudades")
    parser.add_argument("--bootstrap", default="localhost:9092")
    args = parser.parse_args()
    signal.signal(signal.SIGINT, signal.default_int_handler)  # Ctrl+C detiene y muestra el resumen

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "broker.address.family": "v4",
        "group.id": args.grupo,
        "auto.offset.reset": "earliest",  # un grupo nuevo empieza desde el primer mensaje
    })
    try:
        consumer.list_topics(timeout=10)
    except Exception:
        raise SystemExit(f"✗ No hay conexión con Kafka en {args.bootstrap}. ¿Está corriendo? "
                         "docker compose -f .docker/docker-compose.yml up -d kafka")

    def asignadas(_, particiones):
        console.print(f"[bold yellow]» {args.nombre}: me asignaron las particiones "
                      f"{sorted(p.partition for p in particiones) or 'ninguna'}[/]")

    consumer.subscribe([args.topic], on_assign=asignadas)
    conteo: dict[tuple[str, int], int] = {}
    ultimo_seq: dict[tuple[str, str], int] = {}  # (corrida del productor, ciudad) → último seq
    desorden: dict[str, int] = {}

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            pago = json.loads(msg.value())
            ciudad, p = pago["ciudad"], msg.partition()
            conteo[(ciudad, p)] = conteo.get((ciudad, p), 0) + 1
            llave = (pago["run"], ciudad)
            fuera_de_orden = pago["seq"] < ultimo_seq.get(llave, 0)
            if fuera_de_orden:
                desorden[ciudad] = desorden.get(ciudad, 0) + 1
            ultimo_seq[llave] = max(pago["seq"], ultimo_seq.get(llave, 0))
            marca = "  [bold red]⚠ fuera de orden[/]" if fuera_de_orden else ""
            console.print(f"[dim]{args.nombre}[/] [p{p} offset {msg.offset():>5}] "
                          f"[{COLOR[ciudad]}]{ciudad:13s}[/] #{pago['seq']:<5} "
                          f"${pago['amount_cop']:>12,.0f}{marca}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    particiones = sorted({p for _, p in conteo})
    tabla = Table(title=f"Consumidor {args.nombre} · grupo '{args.grupo}'")
    tabla.add_column("Ciudad")
    for p in particiones:
        tabla.add_column(f"p{p}", justify="right")
    tabla.add_column("fuera de orden", justify="right")
    for ciudad in COLOR:
        if any(c == ciudad for c, _ in conteo):
            tabla.add_row(f"[{COLOR[ciudad]}]{ciudad}[/]",
                          *[str(conteo.get((ciudad, p), "")) for p in particiones],
                          str(desorden.get(ciudad, 0)))
    console.print(tabla)


if __name__ == "__main__":
    main()
