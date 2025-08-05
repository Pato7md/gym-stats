import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import plotly.express as px

load_dotenv()

user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
db_name = os.getenv('DB_NAME_GYM')

connection_string = f'postgresql://{user}:{password}@{host}:5432/{db_name}'
engine = create_engine(connection_string, connect_args={"options": "-c client_encoding=utf8"})

def load_data():
    query = 'SELECT * FROM gym_log'
    df = pd.read_sql(query, engine)

    df.rename(columns={'tab_name': 'person', 'timestamp': 'datum'}, inplace=True)
    df['datum'] = pd.to_datetime(df['datum'], dayfirst=True, errors='coerce')

    # Berechnungen wie im Notebook
    for i in [1, 2, 3]:
        df[f'vol_satz{i}'] = df[f'satz{i}_gew'] * df[f'satz{i}_wdh']

    df['vol_gesamt'] = df[[f'vol_satz{i}' for i in [1, 2, 3]]].sum(axis=1)
    df['avg_gewicht'] = df[[f'satz{i}_gew' for i in [1, 2, 3]]].mean(axis=1)
    df['avg_wdh'] = df[[f'satz{i}_wdh' for i in [1, 2, 3]]].mean(axis=1)

    return df

def get_geraete_options(df, person, gym, start, end):
    start_dt = pd.to_datetime(start)
    end_dt = pd.to_datetime(end) + pd.Timedelta(days=1)

    filtered = df[
        (df['person'].str.lower() == person.lower()) &
        (df['gym'] == gym) &
        (df['datum'] >= start_dt) &
        (df['datum'] < end_dt)
    ]

    return sorted(filtered['gerät'].dropna().unique().tolist())

def get_filtered_data(df, person, gym, geraete, start, end):
    start_dt = pd.to_datetime(start)
    end_dt = pd.to_datetime(end) + pd.Timedelta(days=1)

    return df[
        (df['person'].str.lower() == person.lower()) &
        (df['gym'] == gym) &
        (df['gerät'].isin(geraete)) &
        (df['datum'] >= start_dt) &
        (df['datum'] < end_dt)
    ].copy()

def get_aggregated_data(df, metric):
    if df.empty:
        return pd.DataFrame(), None

    df['tag'] = df['datum'].dt.floor('D')

    if metric == 'volumen':
        agg = df.groupby(['tag', 'gerät'])['vol_gesamt'].sum().reset_index()
        y = 'vol_gesamt'
        title = 'Trainingsvolumen pro Gerät über die Zeit'
        y_label = 'Volumen (Gewicht×Wdh)'
    elif metric == 'gewicht':
        agg = df.groupby(['tag', 'gerät'])['avg_gewicht'].mean().reset_index()
        y = 'avg_gewicht'
        title = 'Mittleres Gewicht je Gerät über die Zeit'
        y_label = 'Gewicht'
    elif metric == 'wdh':
        agg = df.groupby(['tag', 'gerät'])['avg_wdh'].mean().reset_index()
        y = 'avg_wdh'
        title = 'Mittlere Wiederholungen je Gerät über die Zeit'
        y_label = 'Wiederholungen'
    else:
        return pd.DataFrame(), None

    fig = px.line(
        agg,
        x='tag',
        y=y,
        color='gerät',
        title=title,
        labels={y: y_label, 'tag': 'Datum', 'gerät': 'Gerät'}
    )
    fig.update_layout(legend_title_text='Gerät')

    return agg, fig
