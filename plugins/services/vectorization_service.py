from langchain_community.embeddings import HuggingFaceInferenceAPIEmbeddings
from dotenv import load_dotenv
import os
import json
import logging


load_dotenv()
api_key = os.getenv("HUGGINGFACE_API_KEY")


def create_embedding(
    texts,
    embedding_model_name="guidecare/all-mpnet-base-v2-feature-extraction",
    hf_api_key=None,
):
    embedding_client = HuggingFaceInferenceAPIEmbeddings(
        api_key=hf_api_key if hf_api_key is not None else api_key,
        model_name=embedding_model_name,
    )
    embeddings = embedding_client.embed_documents(texts)

    return embeddings


def vectorize_knowledge_assets(
    model,
    json_ld_data,
    embedding_model_name,
    use_case=None,
    llm_api_key=None,
    hf_api_key=None,
):
    metadatas = []
    texts = []

    for item in json_ld_data:
        # Choose fields with llm help - future feature
        # fields_to_vectorize = get_fields_to_vectorize(model, item, llm_api_key)
        # fields = json.loads(fields_to_vectorize)

        if use_case is not None:
            pass
        else:
            fields = ["hasPart", "abstract"]

            for field in fields:

                field_value = item.get(field, None)

                if isinstance(field_value, str):
                    # If the field value is a string, append it directly
                    texts.append(field_value)
                    metadatas.append(
                        {"ual": item.get("ual", ""), "name": item.get("name", "")}
                    )

                elif isinstance(field_value, list):
                    # If the field value is a list, iterate through the list
                    for element in field_value:
                        if isinstance(element, str):
                            # If each element is a string, append it
                            texts.append(element)
                            metadatas.append(
                                {
                                    "ual": item.get("ual", ""),
                                    "name": item.get("name", ""),
                                }
                            )
                        elif isinstance(element, dict) and "text" in element:
                            # If each element is a dictionary with a "text" key, append the value of "text"
                            texts.append(element["text"])
                            metadatas.append(
                                {
                                    "ual": item.get("ual", ""),
                                    "name": item.get("name", ""),
                                }
                            )

    if len(texts) > 0:
        embeddings = create_embedding(texts, embedding_model_name, hf_api_key)
    logging.info({"embeddings": embeddings, "metadatas": metadatas, "texts": texts})
    return {"embeddings": embeddings, "metadatas": metadatas, "texts": texts}


# To test
if __name__ == "__main__":
    result = vectorize_knowledge_assets(
        "gpt-4o",
        [
            {
                "@context": {
                    "bc": "https://brickschema.org/schema/Brick#",
                    "schema": "http://schema.org/",
                },
                "@type": "bc:Sensor",
                "@id": "urn:sensor_temperature_eiffel_tower",
                "name": "Temperature Sensor",
                "description": "Temperature Sensor",
                "bc:type": "temperature",
                "bc:measures": "temperature",
                "bc:installedAt": {"@id": "urn:buildingcomponent_first_floor"},
                "bc:status": "active",
                "ual": "did:dkg:otp:2043/0x5cac41237127f94c2d21dae0b14bfefa99880630/6730715",
            },
            {
                "@context": {
                    "bc": "https://brickschema.org/schema/Brick#",
                    "schema": "http://schema.org/",
                },
                "@type": "bc:Sensor",
                "@id": "urn:sensor_temperature_eiffel_tower",
                "name": "Temperature Sensor",
                "description": "Temperature Sensor",
                "bc:type": "temperature",
                "bc:measures": "temperature",
                "bc:installedAt": {"@id": "urn:buildingcomponent_first_floor"},
                "bc:status": "active",
                "ual": "did:dkg:otp:2043/0x5cac41237127f94c2d21dae0b14bfefa99880630/6730715",
            },
        ],
        "guidecare/all-mpnet-base-v2-feature-extraction",
    )

    print(str(result))
