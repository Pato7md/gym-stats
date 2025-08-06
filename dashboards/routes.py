from flask import Blueprint, render_template, request
from .data import load_data, get_geraete_options, get_filtered_data, get_aggregated_data

gym_bp = Blueprint('gym', __name__, template_folder='../../templates')

df = load_data()

@gym_bp.route('/')
def dashboard():
    # Parameter aus URL holen oder Standardwerte setzen
    personen = sorted(df['person'].dropna().unique().tolist())
    gyms = sorted(df['gym'].dropna().unique().tolist())

    person = request.args.get('person', personen[0])
    gym = request.args.get('gym', gyms[0])
    start = request.args.get('start', df['datum'].min().date().isoformat())
    end = request.args.get('end', df['datum'].max().date().isoformat())
    metric = request.args.get('metric', 'volumen')

    geraete = request.args.getlist('geraete')
    if not geraete:
        geraete = get_geraete_options(df, person, gym, start, end)
        if not geraete:
            geraete = []

    filtered_df = get_filtered_data(df, person, gym, geraete, start, end)
    agg, fig = get_aggregated_data(filtered_df, metric)

    plot_html = fig.to_html(full_html=False) if fig else ''

    # Tabelle als Liste von Dicts für Template
    table = agg.to_dict('records') if not agg.empty else []

    return render_template('gym.html',
                           personen=personen,
                           gyms=gyms,
                           person=person,
                           gym=gym,
                           start=start,
                           end=end,
                           metric=metric,
                           geraete=geraete,
                           geraete_options=get_geraete_options(df, person, gym, start, end),
                           table=table,
                           plot_html=plot_html
    )
