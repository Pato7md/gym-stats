from sqlalchemy import text
from datetime import datetime
from db import ENGINE

def insert_gym_entry(*, person, gym, geraet, datum,
                     satz1_gew=None, satz1_wdh=None,
                     satz2_gew=None, satz2_wdh=None,
                     satz3_gew=None, satz3_wdh=None,
                     note=None) -> int | None:
    """Fügt einen Eintrag in gym_log ein und gibt optional die ID zurück."""
    if isinstance(datum, str):
        datum = datetime.fromisoformat(datum)

    sql = text("""
        INSERT INTO gym_log (
            tab_name, gym, gerät, timestamp,
            satz1_gew, satz1_wdh,
            satz2_gew, satz2_wdh,
            satz3_gew, satz3_wdh,
            note
        ) VALUES (
            :person, :gym, :geraet, :datum,
            :satz1_gew, :satz1_wdh,
            :satz2_gew, :satz2_wdh,
            :satz3_gew, :satz3_wdh,
            :note
        )
        RETURNING id
    """)

    with ENGINE.begin() as conn:  # Transaktion (commit/rollback) automatisch
        res = conn.execute(sql, {
            "person": person, "gym": gym, "geraet": geraet, "datum": datum,
            "satz1_gew": satz1_gew, "satz1_wdh": satz1_wdh,
            "satz2_gew": satz2_gew, "satz2_wdh": satz2_wdh,
            "satz3_gew": satz3_gew, "satz3_wdh": satz3_wdh,
            "note": note
        })
        row = res.first()
        return row[0] if row else None
