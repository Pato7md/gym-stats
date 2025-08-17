import pandas as pd
from sqlalchemy import TIMESTAMP, text
from db import ENGINE

sheet_id = "1PIZZOn5x9xRqX1LJbvi2cKbdQUz8t38Dy8pgVUyNkck"
tab_names = ["Michi", "Lutz", "Armin"]
tab_gids = ["0", "728139573", "1662109025"]

with ENGINE.begin() as conn:
    # Tabelle neu erstellen (falls sie schon existiert -> löschen)
    conn.execute(text("DROP TABLE IF EXISTS gym_log CASCADE;"))
    conn.execute(text("""
        CREATE TABLE gym_log (
            id SERIAL PRIMARY KEY,
            person_id INT REFERENCES persons(id),
            gym TEXT,
            gerät TEXT,
            sätze INT,
            satz1_gew FLOAT,
            satz1_wdh INT,
            satz2_gew FLOAT,
            satz2_wdh INT,
            satz3_gew FLOAT,
            satz3_wdh INT,
            timestamp TIMESTAMP,
            tab_name TEXT
        );
    """))

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
            errors="coerce",
            dayfirst=True
        )

    # Tab-Name als Spalte hinzufügen
    df["tab_name"] = tab

    # Person-ID ermitteln
    with ENGINE.begin() as conn:
        res = conn.execute(text("SELECT id FROM persons WHERE name = :n"), {"n": tab})
        row = res.first()
        if row:
            df["person_id"] = row[0]
        else:
            raise ValueError(f"Person {tab} nicht in persons-Tabelle gefunden!")

    # Kommas in Dezimalzahlen umwandeln → Punkt
    for col in df.columns:
        if df[col].dtype == object:  # nur Strings
            df[col] = df[col].str.replace(",", ".", regex=False)

    # Danach in float konvertieren (nur wo es sinnvoll ist)
    num_cols = ["satz1_gew", "satz2_gew", "satz3_gew"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")


    # In DB schreiben (immer append, da Tabelle schon existiert)
    df.to_sql(
        "gym_log",
        ENGINE,
        schema="public",
        if_exists="append",
        index=False,
        dtype={"timestamp": TIMESTAMP()},
        method="multi",
        chunksize=1000,
    )

    print(f"{len(df)} Zeilen aus Tab '{tab}' importiert.")

print("Fertig!")
