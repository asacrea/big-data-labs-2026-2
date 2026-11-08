"""
Experimento 3 del Lab 2, la partition key (apoyo para el informe, no es un entregable).

Envía 60 segundos de eventos en modo --fast a un topic aparte con otra llave de partición.
No modifica producer.py: reemplaza partition_key solo dentro de este proceso. KafkaSink.send
busca partition_key en el módulo cada vez que envía, así que toma la llave nueva.

Uso (desde la raíz del repositorio):
    uv run python data/analisis/experimento_llave.py constante paytrack.experimento
    uv run python data/analisis/experimento_llave.py transaction_id paytrack.experimento2
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path("labs/02-lakehouse").resolve()))
import producer  # noqa: E402

LLAVES = {
    "constante": lambda evento: "paytrack",
    "transaction_id": lambda evento: str(evento["transaction_id"]),
}

if len(sys.argv) != 3 or sys.argv[1] not in LLAVES:
    raise SystemExit("Uso: experimento_llave.py constante|transaction_id <topic>")

llave, topic = sys.argv[1], sys.argv[2]
producer.partition_key = LLAVES[llave]
sink = producer.KafkaSink("localhost:9092", topic)
producer.produce(sink, producer.EventSource(Path("data/batch1")), rate=20, duration=60, fast=True)
print(f"llave: {llave} · topic: {topic}")
