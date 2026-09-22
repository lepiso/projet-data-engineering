# DAG Airflow : pipeline ETL météo (Open-Meteo -> Elasticsearch -> Kibana).
# extract_data -> transform_data -> validate_data -> load_to_elasticsearch
import logging
import pendulum
import sys
from datetime import timedelta
from pathlib import Path

from airflow.sdk import DAG, task

logger = logging.getLogger(__name__)

# Les scripts (extract.py, transform.py...) sont importés directement, pas relancés
# en sous-processus : on récupère ainsi leurs vraies exceptions et leurs valeurs de retour.
PROJECT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_DIR / "scripts"))

DEFAULT_ARGS = {
    "owner": "joan",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="weather_etl_pipeline",
    description="Pipeline ETL météo : Open-Meteo -> Airflow -> Elasticsearch -> Kibana",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 9, 13, tz="UTC"),
    catchup=False,          # ne pas rejouer les jours passés au premier déclenchement
    max_active_runs=1,      # une seule exécution à la fois : évite deux chargements simultanés
    default_args=DEFAULT_ARGS,
    tags=["data-engineering", "meteo", "elasticsearch"],
) as dag:

    @task(retries=5, retry_delay=timedelta(seconds=30))  # dépend du réseau : plus de tentatives
    def extract_data(**context):
        from extract import extract_weather
        run_date = context["logical_date"].strftime("%Y-%m-%d")
        path = extract_weather(past_days=7, run_date=run_date)
        logger.info("Extraction terminée : %s", path)
        return path

    @task
    def transform_data(raw_path: str, **context):
        from transform import transform_weather
        run_date = context["logical_date"].strftime("%Y-%m-%d")
        # up_to : ne garder que les heures déjà écoulées au moment logique de l'exécution
        # (important en cas de rattrapage d'un jour passé).
        up_to = context["logical_date"] + timedelta(days=1)
        path = transform_weather(raw_path, run_date=run_date, up_to=up_to)
        logger.info("Transformation terminée : %s", path)
        return path

    @task
    def validate_data(clean_path: str):
        # Tâche de contrôle qualité : lève une exception si un contrôle échoue,
        # ce qui empêche load_to_elasticsearch de s'exécuter.
        from validate import validate_weather
        validate_weather(clean_path)
        logger.info("Validation réussie : %s", clean_path)
        return clean_path

    @task
    def load_to_elasticsearch(validated_path: str):
        from load import load_to_elasticsearch as load_weather
        summary = load_weather(validated_path)
        logger.info("Chargement terminé : %s", summary)
        return summary

    raw_path = extract_data()
    clean_path = transform_data(raw_path)
    validated_path = validate_data(clean_path)
    load_to_elasticsearch(validated_path)
