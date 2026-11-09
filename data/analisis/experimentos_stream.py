"""
Análisis de los experimentos de streaming del Lab 2 (apoyo para el informe, no es un entregable).

Lee directamente las tablas Delta de lakehouse-local y, por cada corrida del productor,
reporta eventos, ritmo, ráfagas, alertas, aciertos y precisión, y cuánto tardó cada ráfaga
detectada en tener su primera alerta escrita en gold.fraud_alerts después de su último pago.

Una alerta es un acierto si alguno de los pagos del cliente con event_time dentro de
[window_start, window_end) tiene is_fraud = true (la misma regla del TODO 5 de Athena).

Uso (desde la raíz del repositorio):
    uv run python data/analisis/experimentos_stream.py
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

BASE = Path("lakehouse-local/lakehouse")
PAUSA_ENTRE_CORRIDAS = pd.Timedelta(seconds=60)
WATERMARK_S = 120  # WATERMARK de jobs/stream.py ("2 minutes")


def leer_delta(ruta: Path, columnas: list[str]) -> pd.DataFrame:
    """Lee los archivos vigentes de una tabla Delta y agrega commit_ts: cuándo se escribió cada fila."""
    import pyarrow.parquet as pq

    escrito_en: dict[str, int] = {}
    vigentes: set[str] = set()
    for log in sorted((ruta / "_delta_log").glob("*.json")):
        acciones = [json.loads(linea) for linea in log.read_text().splitlines() if linea.strip()]
        ts = next((a["commitInfo"]["timestamp"] for a in acciones if "commitInfo" in a), None)
        for a in acciones:
            if "add" in a:
                escrito_en[a["add"]["path"]] = ts
                vigentes.add(a["add"]["path"])
            elif "remove" in a:
                vigentes.discard(a["remove"]["path"])
    partes = []
    for archivo in sorted(vigentes):
        df = pq.read_table(ruta / archivo, columns=columnas).to_pandas()
        df["commit_ts"] = pd.to_datetime(escrito_en[archivo], unit="ms", utc=True)
        partes.append(df)
    return pd.concat(partes, ignore_index=True) if partes else pd.DataFrame(columns=columnas + ["commit_ts"])


def a_utc(serie: pd.Series) -> pd.Series:
    return pd.to_datetime(serie, utc=True)


def analizar(bronze: pd.DataFrame, alertas: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    bronze = bronze.drop_duplicates("event_id").copy()  # los reenvíos no cuentan dos veces
    for col in ("event_time", "arrival_ts"):
        bronze[col] = a_utc(bronze[col])
    alertas = alertas.reset_index(drop=True).copy()
    for col in ("window_start", "window_end", "commit_ts"):
        alertas[col] = a_utc(alertas[col])

    # Corridas del productor: una pausa de más de 60 s en la llegada separa una corrida de otra
    bronze = bronze.sort_values("arrival_ts")
    bronze["corrida"] = (bronze["arrival_ts"].diff() > PAUSA_ENTRE_CORRIDAS).cumsum() + 1

    corridas = {}
    for corrida, grupo in bronze.groupby("corrida"):
        llegada = (grupo["arrival_ts"].max() - grupo["arrival_ts"].min()).total_seconds()
        q = grupo["event_time"].quantile([0.05, 0.95])  # sin los tardíos de las puntas
        simulado = (q.iloc[1] - q.iloc[0]).total_seconds() / 0.9
        rapido = simulado > 3 * max(llegada, 1)  # --fast: mucho tiempo de evento, casi nada de llegada
        corridas[corrida] = {
            "modo": "--fast" if rapido else "en vivo",
            "duracion": simulado if rapido else max(llegada, 1),
            "inicio": grupo["arrival_ts"].min(),
        }
    # Una alerta salió en su corrida si quedó escrita antes de que llegara el primer evento de la siguiente
    limites = {c: corridas[c + 1]["inicio"] for c in corridas if c + 1 in corridas}

    pagos = bronze[bronze["event_type"] == "payment"]
    alertas["alerta_id"] = alertas.index
    cruce = alertas.merge(pagos[["customer_id", "event_time", "arrival_ts", "is_fraud", "corrida"]],
                          on="customer_id")
    cruce = cruce[(cruce["event_time"] >= cruce["window_start"]) & (cruce["event_time"] < cruce["window_end"])]
    por_alerta = cruce.groupby("alerta_id").agg(
        corrida=("corrida", lambda s: s.mode().iloc[0]),
        cliente=("customer_id", "first"),
        acierto=("is_fraud", "max"),
        fin_ventana=("window_end", "first"),
        escrita=("commit_ts", "first"),
    )
    # Último pago de la ventana que ya había llegado cuando se escribió la alerta. En los aciertos
    # cuenta el último pago de la ráfaga (sus pagos llegan juntos, con event_time cada 2 s)
    previos = cruce[cruce["arrival_ts"] <= cruce["commit_ts"]]
    de_rafaga = previos[previos["is_fraud"]].groupby("alerta_id")["arrival_ts"].max()
    por_alerta["ultimo_pago"] = de_rafaga.combine_first(previos.groupby("alerta_id")["arrival_ts"].max())
    por_alerta["latencia_s"] = (por_alerta["escrita"] - por_alerta["ultimo_pago"]).dt.total_seconds()
    # La latencia se parte en dos: del último pago al cierre de la ventana, y del cierre a la escritura
    por_alerta["cierre_s"] = (por_alerta["fin_ventana"] - por_alerta["ultimo_pago"]).dt.total_seconds()
    por_alerta["espera_s"] = (por_alerta["escrita"] - por_alerta["fin_ventana"]).dt.total_seconds()
    limite = pd.to_datetime(por_alerta["corrida"].map(limites), utc=True)  # NaT en la última corrida
    por_alerta["en_su_corrida"] = limite.isna() | (por_alerta["escrita"] < limite)

    resumen, latencias = [], []
    for corrida, grupo in bronze.groupby("corrida"):
        info = corridas[corrida]
        suyas = por_alerta[por_alerta["corrida"] == corrida]
        a_tiempo = suyas[suyas["en_su_corrida"]]
        aciertos = int(suyas["acierto"].sum())
        en_vivo = info["modo"] == "en vivo"
        # Latencia de cada ráfaga detectada en su corrida: la de su primera alerta (una ráfaga cae en 2 o 3 ventanas)
        detectadas = a_tiempo[a_tiempo["acierto"]]
        primeras = detectadas.loc[detectadas.groupby(["cliente", "ultimo_pago"])["latencia_s"].idxmin()]
        resumen.append({
            "corrida": corrida,
            "modo": info["modo"],
            "inicio": info["inicio"].tz_convert("America/Bogota").strftime("%d %H:%M"),
            "eventos": len(grupo),
            "ev/s": round(len(grupo) / info["duracion"], 1),
            "ráfagas": int(grupo.loc[(grupo["event_type"] == "payment") & grupo["is_fraud"], "customer_id"].nunique()),
            "alertas": len(suyas),
            "aciertos": aciertos,
            "precisión %": round(100 * aciertos / len(suyas), 1) if len(suyas) else None,
            "en su corrida": len(a_tiempo),
            "en otra corrida": len(suyas) - len(a_tiempo),
            "latencia mediana s": round(primeras["latencia_s"].median(), 1) if en_vivo and len(primeras) else None,
        })
        if en_vivo and len(primeras):
            cierre, espera = primeras["cierre_s"].mean(), primeras["espera_s"].mean()
            latencias.append({
                "corrida": corrida,
                "detectadas": len(primeras),
                "mediana s": round(primeras["latencia_s"].median(), 1),
                "promedio s": round(cierre + espera, 1),
                "cierre ventana s": round(cierre, 1),
                "watermark s": WATERMARK_S,
                "micro-lotes s": round(espera - WATERMARK_S, 1),
            })
    return pd.DataFrame(resumen), pd.DataFrame(latencias)


if __name__ == "__main__":
    bronze = leer_delta(BASE / "bronze/transaction_events",
                        ["event_id", "event_type", "event_time", "customer_id", "is_fraud", "arrival_ts"])
    alertas = leer_delta(BASE / "gold/fraud_alerts",
                         ["window_start", "window_end", "customer_id", "num_payments", "total_cop"])
    resumen, latencias = analizar(bronze, alertas)
    pd.set_option("display.width", 120)
    print("Corridas del productor\n")
    print(resumen[["corrida", "modo", "inicio", "eventos", "ev/s", "ráfagas"]].to_string(index=False))
    print("\nAlertas por corrida\n")
    print(resumen[["corrida", "ev/s", "alertas", "aciertos", "precisión %", "en su corrida", "en otra corrida",
                   "latencia mediana s"]].to_string(index=False, na_rep="-"))
    if len(latencias):
        print("\nLatencia de las ráfagas detectadas en su propia corrida: del último pago de la ráfaga")
        print("a su primera alerta en gold.fraud_alerts. promedio = cierre ventana + watermark + micro-lotes\n")
        print(latencias.to_string(index=False))
