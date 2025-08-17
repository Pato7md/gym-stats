from flask import Blueprint, render_template, request, jsonify, Response
import pandas as pd
import plotly.io as pio
import traceback
from importlib import reload
from write_gymdata import insert_gym_entry
from dashboards.prepare_persons import load_persons
import dashboards.prepare_gymdata as prepare_gymdata

gym_bp = Blueprint('gym', __name__, template_folder='../../templates')
df = prepare_gymdata.load_data()


@gym_bp.route('/')
def dashboard():
    personen = load_persons()
    gyms = sorted(df['gym'].dropna().unique().tolist())

    person = request.args.get('person', personen[0])
    gym    = request.args.get('gym', gyms[0] if gyms else '')
    start  = request.args.get('start', df['datum'].min().date().isoformat())
    end    = request.args.get('end', df['datum'].max().date().isoformat())
    metric = request.args.get('metric', 'volumen')

    geraete = request.args.getlist('geraete') or []

    filtered_df = prepare_gymdata.get_filtered_data(df, person, gym, geraete, start, end)
    agg, fig    = prepare_gymdata.get_aggregated_data(filtered_df, metric)

    overview_df = prepare_gymdata.get_filtered_data(df, person, gym, [], start, end)
    overview_stats = prepare_gymdata.get_overview_stats(overview_df, start=start, end=end)

    plot_html = fig.to_html(full_html=False) if fig else '<p>Kein Diagramm vorhanden</p>'
    table = agg.to_dict('records') if not agg.empty else []

    return render_template(
        'gym.html',
        personen=personen, 
        gyms=gyms, 
        person=person, 
        gym=gym,
        start=start, 
        end=end,
        metric=metric,
        geraete=geraete, 
        table=table, 
        plot_html=plot_html,
        overview_stats=overview_stats  
    )


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
        alle_geraete = prepare_gymdata.get_geraete_options(df, person, gym, start, end) or []
        filtered_df = prepare_gymdata.get_filtered_data(df, person, gym, alle_geraete, start, end)
        stats = prepare_gymdata.get_overview_stats(filtered_df, start=start, end=end)

        return render_template('partial_overview.html', stats=stats)

    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': f'Fehler bei Overview-Berechnung: {e}'}), 500


@gym_bp.route('/api/gym-table')
def api_gym_table():
    person = request.args.get('person')
    gym    = request.args.get('gym')
    start  = request.args.get('start')
    end    = request.args.get('end')
    geraete = request.args.getlist('geraete')

    if not start or not end:
        date_df = pd.to_datetime(df.loc[(df['person'] == person) & (df['gym'] == gym), 'datum']).dt.date
        start = date_df.min().isoformat() if not date_df.empty else None
        end = date_df.max().isoformat() if not date_df.empty else None

    if not all([person, gym, start, end]):
        return jsonify({'error': 'Fehlende Parameter'}), 400
    try:
        if not geraete:
            geraete = prepare_gymdata.get_geraete_options(df, person, gym, start, end) or []

        filtered_df = prepare_gymdata.get_filtered_data(df, person, gym, geraete, start, end)
        if filtered_df.empty:
            return jsonify({
                    "table_html": render_template('partial_table.html', table=[]),
                    "min_date": None,
                    "max_date": None
            })

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

        date_df = df[(df['person'] == person) & (df['gym'] == gym)].copy()
        date_df['datum'] = pd.to_datetime(date_df['datum']).dt.date

        min_date = date_df['datum'].min().isoformat() if not date_df.empty else None
        max_date = date_df['datum'].max().isoformat() if not date_df.empty else None

        return jsonify({
            "table_html": render_template('partial_table.html', table=table),
            "min_date": min_date,
            "max_date": max_date
        })
        
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


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

    geraete = prepare_gymdata.get_geraete_options(df, person, gym, start, end) or []
    filtered_df = prepare_gymdata.get_filtered_data(df, person, gym, geraete, start, end)

    if filtered_df.empty:
        return ('', 204)

    agg, fig = prepare_gymdata.get_aggregated_data(filtered_df, metric)
    if not fig or not getattr(fig, 'data', None):
        return ('', 204)
    
    fig.update_layout(title=None, margin=dict(t=24))
    fig.update_traces(mode='lines+markers')
    fig.update_xaxes(tickformat="%Y-%m-%d")
    try:
        return Response(pio.to_json(fig, pretty=False), mimetype='application/json')
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': f'Fehler beim Serialisieren der Grafik: {e}'}), 500


@gym_bp.route('/api/gym-insert', methods=['POST'])
def api_gym_insert():
    try:
        data = request.get_json()

        required_fields = ['person', 'geraet', 'datum']
        for field in required_fields:
            if field not in data or not data[field]:
                return jsonify({"status": "error", "error": f"Fehlendes Feld: {field}"}), 400

        saetze_details = data.get('details', [])

        new_id = insert_gym_entry(
            person=data['person'],
            gym=data.get('gym', ''),
            geraet=data['geraet'],
            saetze=len(saetze_details),
            datum=data['datum'],
            details=saetze_details
        )

        global df
        reload(prepare_gymdata)
        df = prepare_gymdata.load_data()

        return jsonify({"status": "success", "id": new_id})

    except Exception as e:
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(e)}), 500
