from sqlalchemy import text
from db import ENGINE

def load_persons():
    """Alle Personen aus der persons-Tabelle holen."""
    with ENGINE.begin() as conn:
        result = conn.execute(text("SELECT name FROM persons ORDER BY id"))
        return result.scalars().all()
        