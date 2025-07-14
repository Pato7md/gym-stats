import pandas as pd
from sqlalchemy import create_engine, TIMESTAMP
from sqlalchemy.dialects.postgresql import TIMESTAMP


# Google Sheet-ID
sheet_id = "1PIZZOn5x9xRqX1LJbvi2cKbdQUz8t38Dy8pgVUyNkck"

# Tab-Namen und GIDs parallel halten
tab_names = ["Michi", "Lutz", "Armin"]
tab_gids = ["0", "728139573", "1662109025"]

# PostgreSQL-Verbindungsdaten
db_user = 'postgres'
db_password = 'Mic$Tam7373'
db_host = 'localhost'
db_port = '5432'
db_name = 'gym'
db_schema = 'public'

# Verbindung aufbauen mit expliziter Client-Encoding Einstellung
engine = create_engine(
    f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}",
    connect_args={"options": "-c client_encoding=utf8"}
)

for i, gid in enumerate(tab_gids):
    tab = tab_names[i]
    csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    print(f"Lade Tab '{tab}' mit gid '{gid}'...")
    df = pd.read_csv(csv_url, encoding='utf-8')  # Google Sheets Export nutzt meist UTF-8
    
    # Leere Zeilen komplett entfernen
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
    
    # Entferne String-Spalten-Säuberung, da utf-8 bereits gesetzt ist
    
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
        schema=db_schema,
        if_exists=if_exists_option,
        index=False,
        dtype=dtype_mapping
    )
    print(f"{len(df)} Zeilen aus Tab '{tab}' importiert.")

print("Fertig!")
