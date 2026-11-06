"""El productor debe indicar un fallo si el destino no confirmó un evento."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1]))
from producer import produce


def test_entrega_no_confirmada_termina_con_error():
    class Source:
        def next_events(self, now):
            return [{"event_type": "payment"}]

    class Sink:
        def send(self, events):
            return 0

        def close(self):
            return 1

    with pytest.raises(SystemExit, match="1.*sin confirmar"):
        produce(Sink(), Source(), rate=1, duration=1, fast=True)
