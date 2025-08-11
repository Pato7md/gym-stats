from flask import Blueprint, render_template, request, jsonify, Response
from .prepare_gymdata import load_data, get_geraete_options, get_filtered_data, get_aggregated_data, get_overview_stats
import pandas as pd
import plotly.io as pio
import traceback

gym_bp = Blueprint('gym', __name__, template_folder='../../templates')
df = load_data()


# Übergeordnete Route
@gym_bp.route('/')
def dashboard():
    personen = sorted(df['person'].dropna().unique().tolist())
    gyms = sorted(df['gym'].dropna().unique().tolist())

    person = request.args.get('person', personen[1] if len(personen) > 1 else personen[0])
    gym    = request.args.get('gym', gyms[0] if gyms else '')
    start  = request.args.get('start', df['datum'].min().date().isoformat())
    end    = request.args.get('end', df['datum'].max().date().isoformat())
    metric = request.args.get('metric', 'volumen')

    geraete_options = get_geraete_options(df, person, gym, start, end) or []
    geraete = request.args.getlist('geraete') or geraete_options

    filtered_df = get_filtered_data(df, person, gym, geraete, start, end)
    agg, fig    = get_aggregated_data(filtered_df, metric)

    alle_geraete = get_geraete_options(df, person, gym, start,end)or[]
    overview_df = get_filtered_data(df, person, gym, [], start, end)
    overview_stats = get_overview_stats(overview_df, start=start, end=end)

    plot_html = fig.to_html(full_html=False) if fig else '<p>Kein Diagramm vorhanden</p>'
    table = agg.to_dict('records') if not agg.empty else []

    return render_template(
        'gym.html',
        personen=personen, gyms=gyms, person=person, gym=gym,
        start=start, end=end, metric=metric,
        geraete=geraete, geraete_options=geraete_options,
        table=table, plot_html=plot_html,
        overview_stats=overview_stats  
    )


# Geräte Optionen
@gym_bp.route('/api/geraete-options')
def api_geraete_options():
    person = request.args.get('person')
    gym    = request.args.get('gym')
    start  = request.args.get('start')
    end    = request.args.get('end')

    if not all([person, gym, start, end]):
        return jsonify({'error': 'Fehlende Parameter'}), 400

    return jsonify(get_geraete_options(df, person, gym, start, end) or [])


# Tabelle
@gym_bp.route('/api/gym-table')
def api_gym_table():
    person = request.args.get('person')
    gym    = request.args.get('gym')
    start  = request.args.get('start')
    end    = request.args.get('end')
    geraete = request.args.getlist('geraete')

    if not all([person, gym, start, end]):
        return jsonify({'error': 'Fehlende Parameter'}), 400
    try:
        if not geraete:
            geraete = get_geraete_options(df, person, gym, start, end) or []

        filtered_df = get_filtered_data(df, person, gym, geraete, start, end)
        if filtered_df.empty:
            return render_template('partial_table.html', table=[])

        filtered_df['datum'] = pd.to_datetime(filtered_df['datum']).dt.date

        daily = (filtered_df
                 .groupby(['gerät', 'datum'], as_index=False)
                 .agg(volumen_sum=('vol_gesamt','sum'),
                      gewicht_mean=('avg_gewicht','mean'),
                      wdh_mean=('avg_wdh','mean')))

        agg = (daily.groupby('gerät', as_index=False)
               .agg(anzahl_besuche=('datum','nunique'),
                    avg_volumen=('volumen_sum','mean'),
                    avg_gewicht=('gewicht_mean','mean'),
                    avg_wdh=('wdh_mean','mean')))

        agg['avg_volumen'] = agg['avg_volumen'].round(0).astype(int)
        agg['avg_gewicht'] = agg['avg_gewicht'].round(1)
        agg['avg_wdh']     = agg['avg_wdh'].round(1)

        agg = agg.sort_values(['anzahl_besuche','avg_volumen'], ascending=[False, False])

        table = (agg.rename(columns={
                    'gerät':'Gerät','anzahl_besuche':'Anzahl Besuche',
                    'avg_volumen':'Volumen / Besuch','avg_gewicht':'Gewicht / Besuch',
                    'avg_wdh':'Wdh / Besuch'}).to_dict('records'))

        return render_template('partial_table.html', table=table)

    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


# Plot
@gym_bp.route('/api/gym-plot')
def api_gym_plot():
    person = request.args.get('person')
    gym    = request.args.get('gym')
    start  = request.args.get('start')
    end    = request.args.get('end')
    metric = request.args.get('metric')
    geraete = request.args.getlist('geraete')

    if not all([person, gym, start, end, metric]):
        return jsonify({'error': 'Fehlende Parameter'}), 400
    if not geraete:
        return ('', 204)

    filtered_df = get_filtered_data(df, person, gym, geraete, start, end)
    if filtered_df.empty:
        return ('', 204)

    agg, fig = get_aggregated_data(filtered_df, metric)
    if not fig or not getattr(fig, 'data', None):
        return ('', 204)

    fig.update_traces(mode='lines+markers')
    fig.update_xaxes(tickformat="%Y-%m-%d")
    try:
        return Response(pio.to_json(fig, pretty=False), mimetype='application/json')
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': f'Fehler beim Serialisieren der Grafik: {e}'}), 500


# Tabelle 2 Aufruf - User-Stats
@gym_bp.route('/api/gym-overview')
def api_gym_overview():
    person = request.args.get('person')
    gym    = request.args.get('gym')
    start  = request.args.get('start')
    end    = request.args.get('end')
    geraete = request.args.getlist('geraete')

    if not all([person, gym, start, end]):
        return jsonify({'error': 'Fehlende Parameter'}), 400

    try:
        # Wenn keine Geräte übergeben → wie bei Tabelle alle passenden Geräte nehmen
        alle_geraete = get_geraete_options(df, person, gym, start, end) or []
        filtered_df = get_filtered_data(df, person, gym, alle_geraete, start, end)
        stats = get_overview_stats(filtered_df, start=start, end=end)

        # Gibt das Partial zurück; Frontend ersetzt #overview-Inhalt
        return render_template('partial_overview.html', stats=stats)

    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': f'Fehler bei Overview-Berechnung: {e}'}), 500