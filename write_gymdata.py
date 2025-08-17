from sqlalchemy import text
from datetime import datetime
from db import ENGINE

def insert_gym_entry(*, person, gym, geraet, saetze, datum, details=None) -> int | None:
    """Fügt einen Eintrag in gym_log ein und gibt optional die ID zurück."""
    if isinstance(datum, str):
        try:
            datum = datetime.fromisoformat(datum)
        except ValueError:
            datum = datetime.strptime(datum, "%d.%m.%Y")

    values = {}
    for i in range(1, 7):  # 1 bis 6
        satz = next((s for s in (details or []) if s.get("satz") == i), {})
        values[f"satz{i}_gew"] = satz.get("gewicht")
        values[f"satz{i}_wdh"] = satz.get("wdh")

    sql = text("""
        INSERT INTO gym_log (
            person_id, gym, gerät, sätze, timestamp,
            satz1_gew, satz1_wdh,
            satz2_gew, satz2_wdh,
            satz3_gew, satz3_wdh,
            satz4_gew, satz4_wdh,
            satz5_gew, satz5_wdh,
            satz6_gew, satz6_wdh
        ) VALUES (
            (SELECT id FROM persons WHERE name = :person),
            :gym, :geraet, :saetze, :datum,
            :satz1_gew, :satz1_wdh,
            :satz2_gew, :satz2_wdh,
            :satz3_gew, :satz3_wdh,
            :satz4_gew, :satz4_wdh,
            :satz5_gew, :satz5_wdh,
            :satz6_gew, :satz6_wdh
        )
        RETURNING id
    """)

    params = {
        "person": person,
        "gym": gym,
        "geraet": geraet,
        "saetze": saetze,
        "datum": datum,
        **values
    }

    with ENGINE.begin() as conn:
        res = conn.execute(sql, params)
        row = res.first()
        return row[0] if row else None
