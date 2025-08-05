import pandas as pd
from sqlalchemy import create_engine, TIMESTAMP
import os
from dotenv import load_dotenv

#.env Datei laden (nur lokal)
dotenv_path = os.path.join(os.getcwd(), '..', '.env')  # eine Ebene hoch
load_dotenv(dotenv_path)

# Google Sheet
sheet_id = "1PIZZOn5x9xRqX1LJbvi2cKbdQUz8t38Dy8pgVUyNkck"
tab_names = ["Michi", "Lutz", "Armin"]
tab_gids = ["0", "728139573", "1662109025"]


# PostgreSQL-Verbindungsdaten, DB-Verbindung
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
db_name = os.getenv('DB_NAME_GYM')

if not all([user, password, host, db_name]):
    raise ValueError("Eine oder mehrere DB-Umgebungsvariablen fehlen.")

connection_string = f'postgresql://{user}:{password}@{host}:5432/{db_name}'

engine = create_engine(
    connection_string, 
    connect_args={"options": "-c client_encoding=utf8"}
)


for i, gid in enumerate(tab_gids):
    tab = tab_names[i]
    csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    print(f"Lade Tab '{tab}' mit gid '{gid}'...")
    df = pd.read_csv(csv_url, encoding='utf-8')
    
    # Leere Zeilen entfernen
    df = df.dropna(how="all")
    
    if df.empty:
        print(f"Tab '{tab}' ist leer, wird übersprungen.")
        continue
    
    print(f"Anzahl Zeilen in Tab '{tab}': {len(df)}")
    
    # Spalten bereinigen
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    
    # Datum konvertieren, falls vorhanden
    if "tmstmp" in df.columns:
        df["tmstmp"] = pd.to_datetime(df["tmstmp"], errors='coerce')
    
    # Tab-Name als Spalte hinzufügen
    df["tab_name"] = tab
        
    # In DB schreiben — wenn erstes Mal, dann replace, sonst append
    if i == 0:
        if_exists_option = 'replace'
    else:
        if_exists_option = 'append'
    
    # Datentyp Mapping für Timestamp
    dtype_mapping = {}
    if "tmstmp" in df.columns:
        dtype_mapping["tmstmp"] = TIMESTAMP()
    
    df.to_sql(
        "gym_log",
        engine,
        schema="public",  # <- hier ergänzt
        if_exists=if_exists_option,
        index=False,
        dtype=dtype_mapping
    )
    print(f"{len(df)} Zeilen aus Tab '{tab}' importiert.")

print("Fertig!")