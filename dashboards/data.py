import pandas as pd
from sqlalchemy import create_engine
from config import Config

cfg = Config()

def get_engine():
    return create_engine(cfg.db_url, connect_args={"options": "-c client_encoding=utf8"})

def load_raw():
    engine = get_engine()
    df = pd.read_sql("SELECT * FROM gym_log", engine)
    df.rename(columns={"tab_name": "person", "timestamp": "datum"}, inplace=True)
    df["datum"] = pd.to_datetime(df["datum"], dayfirst=True, errors="coerce")
    for i in [1, 2, 3]:
        df[f"vol_satz{i}"] = df[f"satz{i}_gew"] * df[f"satz{i}_wdh"]
    df["vol_gesamt"] = df[[f"vol_satz{i}" for i in [1, 2, 3]]].sum(axis=1)
    df["avg_gewicht"] = df[[f"satz{i}_gew" for i in [1, 2, 3]]].mean(axis=1)
    df["avg_wdh"] = df[[f"satz{i}_wdh" for i in [1, 2, 3]]].mean(axis=1)
    return df

def filter_data(df, person, gym, geraete, start, end):
    start_dt = pd.to_datetime(start)
    end_dt = pd.to_datetime(end) + pd.Timedelta(days=1)
    mask = (
        (df["person"].str.lower() == person.lower()) &
        (df["gym"] == gym) &
        (df["gerät"].isin(geraete)) &
        (df["datum"] >= start_dt) &
        (df["datum"] < end_dt)
    )
    return df[mask].copy()
