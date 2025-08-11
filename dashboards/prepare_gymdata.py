import pandas as pd
import plotly.express as px
from typing import Optional, Dict, Any
from db import ENGINE

def load_data():
    query = 'SELECT * FROM gym_log'
    df = pd.read_sql(query, ENGINE)

    df.rename(columns={'tab_name': 'person', 'timestamp': 'datum'}, inplace=True)
    df['datum'] = pd.to_datetime(df['datum'], dayfirst=True, errors='coerce')

    for col in [f'satz{i}_gew' for i in [1, 2, 3]]:
        df[col] = (
            df[col]
            .astype(str) 
            .str.replace(',', '.', regex=False)
            .str.extract(r'(\d+(?:\.\d+)?)')[0] 
            .astype(float)
        )

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


def _weeks_in_range(range_start: pd.Timestamp, range_end: pd.Timestamp) -> int:
    """
    Anzahl Kalenderwochen im inklusiven Zeitraum (Wochenstart Montag).
    Mindestens 1 zurückgeben, damit die Division stabil bleibt.
    """
    if pd.isna(range_start) or pd.isna(range_end):
        return 1
    if range_end < range_start:
        range_start, range_end = range_end, range_start
    days = pd.date_range(range_start.normalize(), range_end.normalize(), freq='D')
    n_weeks = days.to_period('W-MON').nunique()
    return max(int(n_weeks), 1)


def get_overview_stats(
    filtered_df: pd.DataFrame,
    start: Optional[str] = None,
    end: Optional[str] = None
) -> Dict[str, Any]:
    """
    Berechnet die KPI-Übersicht NUR für die bereits gefilterten Daten.
    Annahmen:
      - 'Besuch' = einzigartiger Trainingstag (unique 'datum' auf Tagesniveau)
      - Ø Besuche/Woche = Besuche / #Kalenderwochen im GEWÄHLTEN Zeitraum (Montag als Wochenstart)
      - Ø Geräte/Besuch = pro Tag distinct 'gerät', davon der Durchschnitt
      - Lieblingsgerät = Gerät mit den meisten Einträgen (Zeilen)
      - Top Wdh/Gewicht = je Gerät: pro Tag mitteln, dann über Tage mitteln -> Top-1
    """
    # Leeres Ergebnis für Edge-Cases
    empty = {
        "total_visits": 0,
        "avg_visits_per_week": 0.0,
        "avg_geraete_per_visit": 0.0,
        "lieblingsgeraet": None,
        "top_wdh_geraet": None,
        "top_wdh_value": None,
        "top_gewicht_geraet": None,
        "top_gewicht_value": None,
    }

    if filtered_df is None or filtered_df.empty:
        return empty

    df = filtered_df.copy()
    df['datum'] = pd.to_datetime(df['datum'], errors='coerce').dt.normalize()

    # 1) Anzahl Besuche gesamt (unique Tage)
    total_visits = int(df['datum'].nunique())

    # 2) Ø-Besuche pro Woche (bezogen auf gewählten Zeitraum)
    if start is not None and end is not None:
        range_start = pd.to_datetime(start, errors='coerce')
        range_end   = pd.to_datetime(end, errors='coerce')
    else:
        range_start = df['datum'].min()
        range_end   = df['datum'].max()
    n_weeks = _weeks_in_range(range_start, range_end)
    avg_visits_per_week = float(total_visits / n_weeks) if n_weeks > 0 else 0.0

    # 3) Ø-Geräte pro Besuch (distinct Geräte je Tag -> Durchschnitt)
    geraete_pro_tag = df.groupby('datum')['gerät'].nunique()
    avg_geraete_per_visit = float(geraete_pro_tag.mean()) if not geraete_pro_tag.empty else 0.0

    # 4) Lieblingsgerät (meiste Einträge/Zeilen)
    if 'gerät' in df.columns and not df['gerät'].dropna().empty:
        counts = df['gerät'].value_counts()
        # Bei Gleichstand alphabetisch erstes auswählen für deterministisches Verhalten
        max_count = counts.max()
        lieblings_candidates = sorted(counts[counts == max_count].index.tolist())
        lieblingsgeraet = lieblings_candidates[0] if lieblings_candidates else None
    else:
        lieblingsgeraet = None

    # 5 & 6) Top-Gerät nach Ø Wdh bzw. Ø Gewicht pro Besuch
    needed = {'gerät', 'datum', 'avg_wdh', 'avg_gewicht'}
    top_wdh_geraet = top_wdh_value = top_gewicht_geraet = top_gewicht_value = None

    if needed.issubset(df.columns) and not df.empty:
        # Pro Gerät & Tag mitteln, dann über Tage mitteln
        per_day = (df.groupby(['gerät', 'datum'], as_index=False)
                     .agg(
                         wdh_day_mean=('avg_wdh', 'mean'),
                         gewicht_day_mean=('avg_gewicht', 'mean'),
                     ))
        per_device = (per_day.groupby('gerät', as_index=False)
                           .agg(
                               wdh_mean_over_days=('wdh_day_mean', 'mean'),
                               gewicht_mean_over_days=('gewicht_day_mean', 'mean'),
                           ))

        if not per_device.empty:
            # Top nach Wdh
            idx_w = per_device['wdh_mean_over_days'].idxmax()
            if pd.notna(idx_w):
                top_wdh_geraet = str(per_device.loc[idx_w, 'gerät'])
                top_wdh_value  = float(round(per_device.loc[idx_w, 'wdh_mean_over_days'], 1))

            # Top nach Gewicht
            idx_g = per_device['gewicht_mean_over_days'].idxmax()
            if pd.notna(idx_g):
                top_gewicht_geraet = str(per_device.loc[idx_g, 'gerät'])
                top_gewicht_value  = float(round(per_device.loc[idx_g, 'gewicht_mean_over_days'], 1))

    return {
        "total_visits": int(total_visits),
        "avg_visits_per_week": float(round(avg_visits_per_week, 2)),
        "avg_geraete_per_visit": float(round(avg_geraete_per_visit, 2)),
        "lieblingsgeraet": lieblingsgeraet,
        "top_wdh_geraet": top_wdh_geraet,
        "top_wdh_value": top_wdh_value,
        "top_gewicht_geraet": top_gewicht_geraet,
        "top_gewicht_value": top_gewicht_value,
    }