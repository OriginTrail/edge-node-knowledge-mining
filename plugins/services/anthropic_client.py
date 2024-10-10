import anthropic
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("ANTHROPIC_API_KEY")


def get_client(api_key=api_key):
    return anthropic.Anthropic(api_key=api_key)


def generate_response(client, prompt, model="claude-3-haiku-20240307", max_tokens=1500):
    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    if response.content:
        return response.content[0].text
    else:
        raise Exception("No response received from Claude.")
