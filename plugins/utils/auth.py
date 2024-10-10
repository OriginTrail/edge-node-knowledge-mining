import os
import requests
from dotenv import load_dotenv
import logging

load_dotenv()

COOKIE_NAME = "connect.sid"
AUTH_ENDPOINT = os.getenv("AUTH_ENDPOINT")


base_user_data = {
    "name": "John",
    "endpoint": os.getenv("OT_NODE_HOSTNAME"),
    "blockchain": {
        "name": "otp:2043",
    },
    "vectorDBUri": os.getenv("VECTOR_DB_URI"),
    "vectorDBUsername": os.getenv("VECTOR_DB_USERNAME"),
    "vectorDBPassword": os.getenv("VECTOR_DB_PASSWORD"),
    "embeddingModelAPIKey": os.getenv("EMBEDDING_MODEL_API_KEY"),
    "embeddingModel": os.getenv("EMBEDDING_MODEL"),
    "provider": "openai",
    "model": "gpt-4o-mini",
    "apiKey": os.getenv("OPEN_AI_KEY"),
    "cohereKey": os.getenv("COHERE_KEY"),
}


def transform_user_config(user_config_list):
    return {config.get("option"): config.get("value") for config in user_config_list}


def authenticate_token(cookie):
    try:
        headers = {"Cookie": f"{COOKIE_NAME}={cookie}"}
        response = requests.get(
            f"{AUTH_ENDPOINT}/auth/check",
            headers=headers,
            cookies={COOKIE_NAME: cookie},
        )
        response.raise_for_status()

        user_config_list = response.json().get("user", {}).get("config", [])
        user_config_dict = transform_user_config(user_config_list)

        user_data = {**base_user_data, **user_config_dict}

        return user_data

    except requests.RequestException as e:
        logging.error(f"Error fetching user config: {e}")
        return None
