import pandas as pd
from sqlalchemy import TIMESTAMP
from db import ENGINE

sheet_id = "1PIZZOn5x9xRqX1LJbvi2cKbdQUz8t38Dy8pgVUyNkck"
tab_names = ["Michi", "Lutz", "Armin"]
tab_gids = ["0", "728139573", "1662109025"]

for i, gid in enumerate(tab_gids):
    tab = tab_names[i]
    csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    print(f"Lade Tab '{tab}' mit gid '{gid}'...")

    df = pd.read_csv(csv_url, encoding='utf-8').dropna(how="all")

    if df.empty:
        print(f"Tab '{tab}' ist leer, wird übersprungen.")
        continue

    print(f"Anzahl Zeilen in Tab '{tab}': {len(df)}")

    # Spalten bereinigen
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Datum konvertieren – egal ob "12.07.2025" oder ISO
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            errors='coerce',
            dayfirst=True   # wichtig: 12.07.2025 = 12. Juli, nicht 7. Dezember
        )


    # Tab-Name als Spalte hinzufügen
    df["tab_name"] = tab

    # In DB schreiben — beim ersten Tab: replace, sonst append
    if_exists_option = 'replace' if i == 0 else 'append'

    # Datentyp-Mapping für Timestamp
    dtype_mapping = {}
    if "tmstmp" in df.columns:
        dtype_mapping["tmstmp"] = TIMESTAMP()

    df.to_sql(
        "gym_log",
        ENGINE,
        schema="public",
        if_exists=if_exists_option,
        index=False,
        dtype=dtype_mapping,
        method="multi",
        chunksize=1000,
    )

    print(f"{len(df)} Zeilen aus Tab '{tab}' importiert.")

print("Fertig!")