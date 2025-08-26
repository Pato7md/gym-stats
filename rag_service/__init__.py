# rag_service/__init__.py

from .embeddings import embed_text
from .db import init_collection, upsert_chunk, search_chunks
