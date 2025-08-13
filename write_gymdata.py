from sqlalchemy import text
from datetime import datetime
from db import ENGINE

def insert_gym_entry(*, person, gym, geraet, saetze, datum,
                     satz1_gew=None, satz1_wdh=None,
                     satz2_gew=None, satz2_wdh=None,
                     satz3_gew=None, satz3_wdh=None) -> int | None:
    """Fügt einen Eintrag in gym_log ein und gibt optional die ID zurück."""
    if isinstance(datum, str):
        try:
            # Erst ISO versuchen
            datum = datetime.fromisoformat(datum)
        except ValueError:
            # Falls nicht ISO, als deutsches Format parsen
            datum = datetime.strptime(datum, "%d.%m.%Y")


    sql = text("""
        INSERT INTO gym_log (
            tab_name, gym, gerät, sätze, timestamp,
            satz1_gew, satz1_wdh,
            satz2_gew, satz2_wdh,
            satz3_gew, satz3_wdh
        ) VALUES (
            :person, :gym, :geraet, :saetze, :datum,
            :satz1_gew, :satz1_wdh,
            :satz2_gew, :satz2_wdh,
            :satz3_gew, :satz3_wdh
        )
        RETURNING id
    """)

    with ENGINE.begin() as conn:  # Transaktion (commit/rollback) automatisch
        res = conn.execute(sql, {
            "person": person, "gym": gym, "geraet": geraet, "saetze": saetze, "datum": datum,
            "satz1_gew": satz1_gew, "satz1_wdh": satz1_wdh,
            "satz2_gew": satz2_gew, "satz2_wdh": satz2_wdh,
            "satz3_gew": satz3_gew, "satz3_wdh": satz3_wdh
        })
        row = res.first()
        return row[0] if row else None
