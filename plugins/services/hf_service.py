import requests
import os

HF_MODEL = "guidecare/all-mpnet-base-v2-feature-extraction"
hf_token = os.environ["HUGGINGFACE_API_KEY"]
api_url = f"https://api-inference.huggingface.co/pipeline/feature-extraction/{HF_MODEL}"
headers = {"Authorization": f"Bearer {hf_token}"}


class HuggingFaceEmbeddingWrapper:
    def __init__(self, api_url, headers):
        self.api_url = api_url
        self.headers = headers

    def embed_documents(self, texts):
        return self.embed(texts)

    def embed(self, texts):
        response = requests.post(
            self.api_url,
            headers=self.headers,
            json={"inputs": texts, "options": {"wait_for_model": True}},
        )
        return response.json()


embeddings = HuggingFaceEmbeddingWrapper(api_url, headers)
