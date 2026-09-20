# Contrôle qualité des données transformées.
# Si un contrôle échoue, la fonction lève ValidationError avec un message clair :
# la tâche Airflow passe alors en échec et le chargement dans Elasticsearch n'a pas lieu.
# Le résultat de chaque contrôle est aussi enregistré dans un rapport JSON.
import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

PROCESSED_DIR = Path(__file__).resolve().parent.parent / "data" / "processed"


class ValidationError(Exception):
    # Erreur levée quand les données ne respectent pas les règles de qualité.
    pass


REQUIRED_COLUMNS = [
    "doc_id", "timestamp", "date", "hour",
    "city", "country", "continent", "hemisphere", "latitude", "longitude",
    "temperature_c", "humidity_pct", "precipitation_mm", "wind_speed_kmh",
    "weather_code", "weather_label", "temperature_category", "is_raining",
]
# Colonnes indispensables : aucune valeur nulle tolérée.
KEY_COLUMNS = ["doc_id", "timestamp", "city"]
NUMERIC_COLUMNS = [
    "hour", "latitude", "longitude", "temperature_c", "humidity_pct",
    "precipitation_mm", "wind_speed_kmh", "weather_code",
]
MIN_ROWS_PER_CITY = 24    # au moins une journée de données par ville
MAX_NULL_RATIO = 0.01     # autres colonnes : 1 % de valeurs nulles toléré
# Valeurs plausibles (minimum, maximum) : au-delà, la donnée est forcément fausse.
VALUE_RANGES = {
    "temperature_c": (-90, 60),
    "humidity_pct": (0, 100),
    "precipitation_mm": (0, 500),
    "wind_speed_kmh": (0, 400),
    "latitude": (-90, 90),
    "longitude": (-180, 180),
    "hour": (0, 23),
}


def _result(name, passed, detail):
    return {"check": name, "passed": bool(passed), "detail": detail}


def check_row_count(df):
    counts = df["city"].value_counts()
    too_small = counts[counts < MIN_ROWS_PER_CITY]
    detail = f"{len(df)} lignes, {len(counts)} villes"
    if not too_small.empty:
        detail += f" ; moins de {MIN_ROWS_PER_CITY} lignes pour : " + ", ".join(too_small.index)
    return _result("nombre de lignes", len(df) > 0 and too_small.empty, detail)


def check_nulls(df):
    ratios = df[REQUIRED_COLUMNS].isna().mean()
    bad = [c for c, ratio in ratios.items() if ratio > (0 if c in KEY_COLUMNS else MAX_NULL_RATIO)]
    if bad:
        detail = "trop de valeurs nulles : " + ", ".join(f"{c} ({ratios[c]:.1%})" for c in bad)
    else:
        detail = f"nulls sous le seuil (max {ratios.max():.2%})"
    return _result("valeurs nulles", not bad, detail)


def check_types(df):
    bad = [c for c in NUMERIC_COLUMNS if not pd.api.types.is_numeric_dtype(df[c])]
    if not pd.api.types.is_bool_dtype(df["is_raining"]):
        bad.append("is_raining")
    detail = "types conformes" if not bad else "types incorrects : " + ", ".join(bad)
    return _result("types des colonnes", not bad, detail)


def check_unique_id(df):
    n_duplicates = int(df["doc_id"].duplicated().sum())
    return _result("unicité de doc_id", n_duplicates == 0, f"{n_duplicates} doublon(s)")


def check_dates(df):
    timestamps = pd.to_datetime(df["timestamp"], format="%Y-%m-%dT%H:%M:%SZ", utc=True, errors="coerce")
    n_invalid = int(timestamps.isna().sum())
    limit = pd.Timestamp.now(tz="UTC") + pd.Timedelta(hours=1)
    n_future = int((timestamps > limit).sum())
    detail = f"{n_invalid} date(s) invalide(s), {n_future} date(s) dans le futur ; période {timestamps.min()} -> {timestamps.max()}"
    return _result("dates valides", n_invalid == 0 and n_future == 0, detail)


def check_ranges(df):
    problems = []
    for column, (low, high) in VALUE_RANGES.items():
        n_out = int(((df[column] < low) | (df[column] > high)).sum())
        if n_out:
            problems.append(f"{column} : {n_out} valeur(s) hors de [{low}, {high}]")
    detail = "toutes les valeurs sont plausibles" if not problems else " ; ".join(problems)
    return _result("valeurs plausibles", not problems, detail)


def validate_weather(csv_path):
    # Vérifie le CSV. Retourne son chemin (str) si tout est bon, sinon lève ValidationError.
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise ValidationError(f"Fichier introuvable : {csv_path}")
    try:
        df = pd.read_csv(csv_path)
    except pd.errors.EmptyDataError:
        raise ValidationError(f"Le fichier est vide : {csv_path}")

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValidationError("Colonnes obligatoires absentes : " + ", ".join(missing))

    checks = [
        check_row_count(df),
        check_nulls(df),
        check_types(df),
        check_unique_id(df),
        check_dates(df),
        check_ranges(df),
    ]
    for check in checks:
        status = "OK    " if check["passed"] else "ÉCHEC "
        log = logger.info if check["passed"] else logger.error
        log("[%s] %s : %s", status, check["check"], check["detail"])

    failed = [c for c in checks if not c["passed"]]
    run_date = csv_path.stem.replace("weather_clean_", "")
    report = {
        "file": csv_path.name,
        "validated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rows": len(df),
        "passed": not failed,
        "checks": checks,
    }
    report_path = csv_path.parent / f"quality_report_{run_date}.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Rapport qualité : %s", report_path)

    if failed:
        raise ValidationError(
            f"{len(failed)} contrôle(s) qualité en échec : "
            + " | ".join(f"{c['check']} -> {c['detail']}" for c in failed)
        )
    logger.info("Validation réussie : %d contrôles OK", len(checks))
    return str(csv_path)


def latest_clean_file():
    files = sorted(PROCESSED_DIR.glob("weather_clean_*.csv"))
    if not files:
        raise ValidationError(f"Aucun CSV dans {PROCESSED_DIR} : lance d'abord transform.py")
    return files[-1]


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Contrôle qualité des données météo")
    parser.add_argument("csv_path", nargs="?", default=None, help="CSV à valider (par défaut : le plus récent)")
    args = parser.parse_args()
    try:
        path = validate_weather(args.csv_path or latest_clean_file())
    except ValidationError as exc:
        logger.error("%s", exc)
        raise SystemExit(1)
    print(f"Données valides : {path}")


if __name__ == "__main__":
    main()
