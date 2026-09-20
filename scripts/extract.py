"""Extraction des données météo horaires depuis l'API publique Open-Meteo.

Pour chaque ville : appel de l'API, vérification de la réponse, puis sauvegarde
du JSON brut dans data/raw/. La fonction principale retourne le chemin du
fichier créé : Airflow transmettra ce chemin (petit) à l'étape suivante via XCom.
"""
import argparse
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

API_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARIABLES = [
    "temperature_2m",        # °C
    "relative_humidity_2m",  # %
    "precipitation",         # mm
    "wind_speed_10m",        # km/h
    "weather_code",          # code météo WMO (0 = ciel dégagé, 61 = pluie...)
]

# Villes suivies :
CITIES = [
    {"city": "Paris", "country": "France", "continent": "Europe", "latitude": 48.8566, "longitude": 2.3522},
    {"city": "Londres", "country": "Royaume-Uni", "continent": "Europe", "latitude": 51.5074, "longitude": -0.1278},
    {"city": "Reykjavik", "country": "Islande", "continent": "Europe", "latitude": 64.1466, "longitude": -21.9426},
    {"city": "New York", "country": "États-Unis", "continent": "Amérique", "latitude": 40.7128, "longitude": -74.0060},
    {"city": "São Paulo", "country": "Brésil", "continent": "Amérique", "latitude": -23.5505, "longitude": -46.6333},
    {"city": "Tokyo", "country": "Japon", "continent": "Asie", "latitude": 35.6762, "longitude": 139.6503},
    {"city": "Dubaï", "country": "Émirats arabes unis", "continent": "Asie", "latitude": 25.2048, "longitude": 55.2708},
    {"city": "Le Caire", "country": "Égypte", "continent": "Afrique", "latitude": 30.0444, "longitude": 31.2357},
    {"city": "Nairobi", "country": "Kenya", "continent": "Afrique", "latitude": -1.2921, "longitude": 36.8219},
    {"city": "Sydney", "country": "Australie", "continent": "Océanie", "latitude": -33.8688, "longitude": 151.2093},
]

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
MAX_ATTEMPTS = 3
TIMEOUT_SECONDS = 30


class ExtractionError(Exception):
    # Erreur claire levée quand l'extraction échoue (la tâche Airflow passera en échec).
    pass


def _api_reason(response):
    # Récupère le message d'erreur renvoyé par l'API, si elle en fournit un.
    try:
        return response.json().get("reason", response.text[:200])
    except ValueError:
        return response.text[:200]


def check_payload(payload):
    # Vérifie que la réponse contient bien les données attendues.
    if not isinstance(payload, dict):
        raise ValueError("la réponse n'est pas un objet JSON")
    hourly = payload.get("hourly")
    if not isinstance(hourly, dict) or not hourly.get("time"):
        raise ValueError("aucune donnée horaire dans la réponse")
    n_hours = len(hourly["time"])
    for variable in HOURLY_VARIABLES:
        values = hourly.get(variable)
        if values is None:
            raise ValueError(f"variable '{variable}' absente de la réponse")
        if len(values) != n_hours:
            raise ValueError(f"variable '{variable}' : {len(values)} valeurs pour {n_hours} heures")


def fetch_city(city, past_days):
    # Appelle l'API pour une ville, avec quelques nouvelles tentatives en cas de problème réseau.
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "hourly": ",".join(HOURLY_VARIABLES),
        "past_days": past_days,
        "forecast_days": 1,
        "timezone": "GMT",  # heures en UTC : évite toute ambiguïté de fuseau horaire
    }
    last_error = "erreur inconnue"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            logger.info("Appel API pour %s (tentative %d/%d)", city["city"], attempt, MAX_ATTEMPTS)
            response = requests.get(API_URL, params=params, timeout=TIMEOUT_SECONDS)
            response.raise_for_status()
            payload = response.json()
            check_payload(payload)
            return payload
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code
            last_error = f"HTTP {status} : {_api_reason(exc.response)}"
            if status < 500 and status != 429:
                break  # notre requête est fausse : réessayer ne servirait à rien
        except ValueError as exc:  # JSON invalide ou contenu inattendu
            last_error = f"réponse invalide : {exc}"
        except requests.exceptions.RequestException as exc:  # timeout, DNS, connexion coupée...
            last_error = f"erreur réseau : {exc}"
        logger.warning("%s : %s", city["city"], last_error)
        if attempt < MAX_ATTEMPTS:
            time.sleep(2 * attempt)
    raise ExtractionError(f"Échec de l'extraction pour {city['city']} : {last_error}")


def extract_weather(past_days=7, run_date=None, output_dir=RAW_DIR):
    """Extrait la météo horaire des dernières `past_days` journées + aujourd'hui.

    Retourne le chemin du fichier JSON brut créé (str).
    """
    if not 0 <= past_days <= 92:
        raise ExtractionError("past_days doit être compris entre 0 et 92")
    run_date = run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    records = []
    for city in CITIES:
        payload = fetch_city(city, past_days)
        records.append({**city, "response": payload})

    raw = {
        "source": API_URL,
        "extracted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "past_days": past_days,
        "cities": records,
    }
    output_path = output_dir / f"weather_raw_{run_date}.json"
    output_path.write_text(json.dumps(raw, ensure_ascii=False), encoding="utf-8")
    logger.info("Extraction terminée : %d villes -> %s", len(records), output_path)
    return str(output_path)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Extraction météo Open-Meteo")
    parser.add_argument("--past-days", type=int, default=7, help="nombre de jours passés (0-92)")
    parser.add_argument("--run-date", default=None, help="date utilisée dans le nom du fichier (AAAA-MM-JJ)")
    args = parser.parse_args()
    try:
        path = extract_weather(args.past_days, args.run_date)
    except ExtractionError as exc:
        logger.error("%s", exc)
        raise SystemExit(1)
    print(f"Fichier créé : {path}")


if __name__ == "__main__":
    main()
