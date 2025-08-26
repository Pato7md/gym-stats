# rag_service/watcher.py
import os
import hashlib
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Korrekte Imports
from rag_service.embeddings import embed_text
from rag_service.db import upsert_chunk, init_collection


WATCH_PATHS = ["dashboards", "app.py", "db.py"]  # Ordner, die überwacht werden sollen
FILE_EXTENSIONS = [".py"]  # Nur Python-Skripte

def file_to_chunks(filepath: str, chunk_size: int = 500):
    """
    Liest eine Datei und teilt sie in Text-Chunks.
    """
    with open(filepath, "r", encoding="utf-8") as f:
        text = f.read()

    # Einfacher Split in Blöcke von `chunk_size` Zeichen
    for i in range(0, len(text), chunk_size):
        yield text[i:i+chunk_size]

class CodeChangeHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.is_directory:
            return

        filepath = os.path.normpath(event.src_path)
        if not any(filepath.endswith(ext) for ext in FILE_EXTENSIONS):
            return

        print(f"🔄 Änderung erkannt: {filepath}")

        for idx, chunk in enumerate(file_to_chunks(filepath)):
            embedding = embed_text(chunk)
            upsert_chunk(f"{filepath}_{idx}", embedding, chunk, filepath)
            print(f"✅ Chunk {idx} von {filepath} in DB gespeichert")  # <-- gehört HIER in die Schleife


def start_watcher():
    init_collection()
    event_handler = CodeChangeHandler()
    observer = Observer()

    for path in WATCH_PATHS:
        if os.path.exists(path):
            observer.schedule(event_handler, path=path, recursive=True)

    observer.start()
    print("👀 Watcher gestartet – Änderungen an .py-Dateien werden verfolgt")
    try:
        while True:
            pass
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    start_watcher()
