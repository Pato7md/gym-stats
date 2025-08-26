# rag_service/embeddings.py
import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def embed_text(text: str):
    """
    Erzeugt ein Embedding mit der neuen OpenAI API.
    """
    response = client.embeddings.create(
        model="text-embedding-3-large",
        input=text
    )
    return response.data[0].embedding
