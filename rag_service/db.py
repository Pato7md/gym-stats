# rag_service/db.py
import chromadb
from chromadb.config import Settings

COLLECTION_NAME = "scripts"

# Lokaler Client, speichert Daten im Ordner ./chromadb_data
client = chromadb.PersistentClient(path="chromadb_data")

def init_collection():
    """
    Erstellt eine Collection, falls sie noch nicht existiert.
    """
    try:
        client.get_collection(COLLECTION_NAME)
    except:
        client.create_collection(COLLECTION_NAME)

def upsert_chunk(chunk_id: str, embedding: list, text: str, filepath: str):
    """
    Fügt einen Chunk in die DB ein oder ersetzt ihn.
    """
    collection = client.get_collection(COLLECTION_NAME)
    collection.upsert(
        ids=[chunk_id],
        embeddings=[embedding],
        documents=[text],
        metadatas=[{"filepath": filepath}]
    )

def search_chunks(query_vector: list, limit: int = 3):
    """
    Sucht semantisch ähnliche Chunks in der DB.
    """
    collection = client.get_collection(COLLECTION_NAME)
    results = collection.query(
        query_embeddings=[query_vector],
        n_results=limit
    )
    return results
