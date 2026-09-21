# Chargement des données propres dans Elasticsearch.
# - se connecte à Elasticsearch (configuration lue dans des variables d'environnement,
#   ou fournie directement par Airflow via une Connection) ;
# - crée l'index avec son mapping (elasticsearch/mapping.json) s'il n'existe pas ;
# - charge les documents avec l'API bulk. L'_id d'un document est son doc_id :
#   recharger le même fichier met les documents à jour, sans créer de doublons.
import argparse
import json
import logging
import os
from pathlib import Path

import pandas as pd
from elastic_transport import ConnectionError as TransportConnectionError
from elastic_transport import TlsError
from elasticsearch import ApiError, AuthenticationException, Elasticsearch, helpers

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"
MAPPING_PATH = BASE_DIR / "elasticsearch" / "mapping.json"
INDEX_NAME = "weather-observations"


class LoadError(Exception):
    # Erreur claire levée quand la connexion ou le chargement échoue.
    pass


def config_from_env():
    # Lit la configuration de connexion dans les variables d'environnement (voir .env.example).
    return {
        "host": os.environ.get("ES_HOST", "localhost"),
        "port": int(os.environ.get("ES_PORT", "9200")),
        "scheme": os.environ.get("ES_SCHEME", "https"),
        "user": os.environ.get("ES_USER"),
        "password": os.environ.get("ES_PASSWORD"),
        "ca_certs": os.environ.get("ES_CA_CERTS"),
        "verify_certs": os.environ.get("ES_VERIFY_CERTS", "true").lower() != "false",
    }


def get_client(config):
    url = f"{config['scheme']}://{config['host']}:{config['port']}"
    options = {"request_timeout": 30}
    if config.get("user"):
        options["basic_auth"] = (config["user"], config.get("password") or "")
    if config["scheme"] == "https":
        options["verify_certs"] = config.get("verify_certs", True)
        if config.get("ca_certs"):
            options["ca_certs"] = config["ca_certs"]
    return Elasticsearch(url, **options)


def check_connection(client, config):
    url = f"{config['scheme']}://{config['host']}:{config['port']}"
    try:
        info = client.info()
    except AuthenticationException:
        raise LoadError(f"Authentification refusée par {url} : vérifie l'utilisateur et le mot de passe")
    except TlsError as exc:
        raise LoadError(f"Erreur de certificat TLS avec {url} : vérifie le chemin du certificat CA ({exc})")
    except TransportConnectionError as exc:
        raise LoadError(f"Impossible de joindre Elasticsearch sur {url} : est-il démarré ? ({exc})")
    except ApiError as exc:
        raise LoadError(f"Erreur renvoyée par Elasticsearch : {exc}")
    logger.info("Connecté à Elasticsearch %s (%s)", info["version"]["number"], url)


def ensure_index(client, index, recreate=False):
    # Crée l'index avec son mapping s'il n'existe pas (ou le recrée si recreate=True).
    if client.indices.exists(index=index):
        if not recreate:
            logger.info("L'index '%s' existe déjà : on le réutilise", index)
            return
        logger.warning("Suppression de l'index '%s' (recreate=True)", index)
        client.indices.delete(index=index)
    mapping = json.loads(MAPPING_PATH.read_text(encoding="utf-8"))
    client.indices.create(index=index, settings=mapping["settings"], mappings=mapping["mappings"])
    logger.info("Index '%s' créé avec son mapping", index)


def build_actions(df, index):
    # Transforme chaque ligne du tableau en une action « indexer ce document ».
    for record in df.to_dict(orient="records"):
        record["location"] = {"lat": record["latitude"], "lon": record["longitude"]}
        yield {"_index": index, "_id": record["doc_id"], "_source": record}


def load_to_elasticsearch(csv_path, config=None, index=INDEX_NAME, recreate=False):
    # Charge le CSV dans l'index. Retourne un petit résumé (dict) exploitable via XCom.
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise LoadError(f"Fichier introuvable : {csv_path}")
    config = config or config_from_env()
    client = get_client(config)
    check_connection(client, config)
    ensure_index(client, index, recreate)

    df = pd.read_csv(csv_path)
    logger.info("Chargement de %d documents depuis %s", len(df), csv_path.name)
    try:
        n_ok, _ = helpers.bulk(client, build_actions(df, index), chunk_size=500)
    except helpers.BulkIndexError as exc:
        first = exc.errors[0] if exc.errors else "inconnu"
        raise LoadError(f"{len(exc.errors)} document(s) rejeté(s) par Elasticsearch. Premier rejet : {first}") from exc
    client.indices.refresh(index=index)  # rend les documents immédiatement recherchables
    total = client.count(index=index)["count"]
    logger.info("Chargement terminé : %d documents envoyés, %d dans l'index '%s'", n_ok, total, index)
    return {"index": index, "loaded": n_ok, "total_in_index": total}


def latest_clean_file():
    files = sorted(PROCESSED_DIR.glob("weather_clean_*.csv"))
    if not files:
        raise LoadError(f"Aucun CSV dans {PROCESSED_DIR} : lance d'abord transform.py")
    return files[-1]


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Chargement dans Elasticsearch")
    parser.add_argument("csv_path", nargs="?", default=None, help="CSV à charger (par défaut : le plus récent)")
    parser.add_argument("--recreate", action="store_true", help="supprime puis recrée l'index avant le chargement")
    args = parser.parse_args()
    try:
        summary = load_to_elasticsearch(args.csv_path or latest_clean_file(), recreate=args.recreate)
    except LoadError as exc:
        logger.error("%s", exc)
        raise SystemExit(1)
    print(f"Chargement réussi : {summary}")


if __name__ == "__main__":
    main()
