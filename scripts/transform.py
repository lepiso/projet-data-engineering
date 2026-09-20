# Transformation des données météo brutes (JSON) en un tableau propre (CSV).
# Une ligne = une ville à une heure donnée.
# La fonction principale retourne le chemin du CSV créé : Airflow le transmettra
# à l'étape suivante (validation) via XCom.
import argparse
import json
import logging
import unicodedata
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"


class TransformationError(Exception):
    # Erreur claire levée quand la transformation échoue (la tâche Airflow passera en échec).
    pass


# Noms des colonnes de l'API -> noms explicites avec l'unité.
RENAME = {
    "temperature_2m": "temperature_c",
    "relative_humidity_2m": "humidity_pct",
    "precipitation": "precipitation_mm",
    "wind_speed_10m": "wind_speed_kmh",
}
MEASURES = ["temperature_c", "humidity_pct", "precipitation_mm", "wind_speed_kmh"]
# Grandeurs continues : on peut combler un petit trou par interpolation (pas la pluie).
INTERPOLATED = ["temperature_c", "humidity_pct", "wind_speed_kmh"]

# Codes météo WMO regroupés en catégories lisibles (pour les graphiques Kibana).
WEATHER_GROUPS = {
    "Dégagé": [0, 1],
    "Nuageux": [2, 3],
    "Brouillard": [45, 48],
    "Bruine": [51, 53, 55, 56, 57],
    "Pluie": [61, 63, 65, 66, 67, 80, 81, 82],
    "Neige": [71, 73, 75, 77, 85, 86],
    "Orage": [95, 96, 99],
}
CODE_TO_LABEL = {code: label for label, codes in WEATHER_GROUPS.items() for code in codes}

FINAL_COLUMNS = [
    "doc_id", "timestamp", "date", "hour",
    "city", "country", "continent", "hemisphere", "latitude", "longitude",
    "temperature_c", "humidity_pct", "precipitation_mm", "wind_speed_kmh",
    "weather_code", "weather_label", "temperature_category", "is_raining",
]


def slugify(text):
    # "São Paulo" -> "sao-paulo" (sert à fabriquer un identifiant sans accents ni espaces)
    ascii_text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    return ascii_text.lower().replace(" ", "-")


def flatten(raw):
    # Transformation 1 : aplatir le JSON imbriqué en tableau (une ligne par ville et par heure).
    frames = []
    for entry in raw["cities"]:
        frame = pd.DataFrame(entry["response"]["hourly"])
        for key in ("city", "country", "continent", "latitude", "longitude"):
            frame[key] = entry[key]
        frames.append(frame)
    if not frames:
        raise TransformationError("Le fichier brut ne contient aucune ville")
    return pd.concat(frames, ignore_index=True).rename(columns=RENAME)


def convert_types(df):
    # Transformation 2 : convertir les types et normaliser les dates en UTC.
    df["timestamp"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df = df.drop(columns="time")
    for column in MEASURES + ["weather_code"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def handle_missing(df):
    # Transformation 3 : valeurs manquantes.
    # a) petits trous (3 h max) des grandeurs continues : interpolation linéaire par ville ;
    # b) lignes encore incomplètes : supprimées.
    df = df.sort_values(["city", "timestamp"]).reset_index(drop=True)
    df[INTERPOLATED] = df.groupby("city")[INTERPOLATED].transform(
        lambda s: s.interpolate(limit=3, limit_area="inside")
    )
    required = ["timestamp"] + MEASURES + ["weather_code"]
    n_missing_cells = int(df[required].isna().sum().sum())
    df = df.dropna(subset=required).reset_index(drop=True)
    df["weather_code"] = df["weather_code"].astype("int64")
    return df, n_missing_cells


def add_columns(df):
    # Transformation 5 : créer de nouvelles colonnes utiles à l'analyse.
    df["date"] = df["timestamp"].dt.strftime("%Y-%m-%d")
    df["hour"] = df["timestamp"].dt.hour
    df["hemisphere"] = (df["latitude"] >= 0).map({True: "Nord", False: "Sud"})
    df["weather_label"] = df["weather_code"].map(CODE_TO_LABEL).fillna("Autre")
    df["temperature_category"] = pd.cut(
        df["temperature_c"],
        bins=[-float("inf"), 0, 10, 20, 30, float("inf")],
        labels=["Gel", "Froid", "Doux", "Chaud", "Très chaud"],
        right=False,
    ).astype(str)
    df["is_raining"] = df["precipitation_mm"] >= 0.1
    df[MEASURES] = df[MEASURES].round(1)
    # Identifiant unique d'un document : ville + heure (sert aussi d'_id dans Elasticsearch).
    df["doc_id"] = df["city"].map(slugify) + "_" + df["timestamp"].dt.strftime("%Y%m%dT%H")
    return df


def transform_weather(raw_path, run_date=None, up_to=None, output_dir=PROCESSED_DIR):
    # Lit le JSON brut, applique les transformations et retourne le chemin du CSV propre (str).
    raw_path = Path(raw_path)
    if not raw_path.exists():
        raise TransformationError(f"Fichier brut introuvable : {raw_path}")
    run_date = run_date or raw_path.stem.replace("weather_raw_", "")
    up_to = pd.Timestamp(up_to) if up_to is not None else pd.Timestamp.now(tz="UTC")
    if up_to.tzinfo is None:
        up_to = up_to.tz_localize("UTC")

    raw = json.loads(raw_path.read_text(encoding="utf-8"))

    df = flatten(raw)
    logger.info("Après aplatissement : %d lignes", len(df))

    df = convert_types(df)
    logger.info("Types convertis et dates normalisées (UTC)")

    # Transformation 4 : supprimer les doublons (même ville, même heure).
    n_before = len(df)
    df = df.drop_duplicates(subset=["city", "timestamp"], keep="last")
    logger.info("Doublons supprimés : %d", n_before - len(df))

    df, n_missing = handle_missing(df)
    logger.info("Cellules manquantes rencontrées : %d -> lignes restantes : %d", n_missing, len(df))

    # Transformation 6 : filtrer. On garde uniquement les heures déjà écoulées
    # (les heures à venir d'aujourd'hui sont des prévisions, pas des mesures).
    n_before = len(df)
    df = df[df["timestamp"] <= up_to].reset_index(drop=True)
    logger.info("Heures futures écartées : %d", n_before - len(df))

    if df.empty:
        raise TransformationError("Aucune ligne exploitable après transformation")

    df = add_columns(df)

    out = df[FINAL_COLUMNS].copy()
    out["timestamp"] = out["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"weather_clean_{run_date}.csv"
    out.to_csv(output_path, index=False)
    logger.info("Transformation terminée : %d lignes -> %s", len(out), output_path)
    return str(output_path)


def latest_raw_file():
    files = sorted(RAW_DIR.glob("weather_raw_*.json"))
    if not files:
        raise TransformationError(f"Aucun fichier brut dans {RAW_DIR} : lance d'abord extract.py")
    return files[-1]


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Transformation des données météo")
    parser.add_argument("raw_path", nargs="?", default=None, help="fichier JSON brut (par défaut : le plus récent)")
    args = parser.parse_args()
    try:
        path = transform_weather(args.raw_path or latest_raw_file())
    except TransformationError as exc:
        logger.error("%s", exc)
        raise SystemExit(1)
    print(f"Fichier créé : {path}")


if __name__ == "__main__":
    main()
