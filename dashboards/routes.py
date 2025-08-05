from flask import Blueprint, render_template, request
from .data import load_raw, filter_data
import plotly.express as px

gym_bp = Blueprint("gym", __name__, template_folder="../../templates")

raw = load_raw()

@gym_bp.route("/")
def dashboard():
    person = request.args.get("person", "michi")
    gym = request.args.get("gym", raw["gym"].dropna().unique()[0])
    geraete = request.args.getlist("geraete") or [raw["gerät"].dropna().unique()[0]]
    start = request.args.get("start", raw["datum"].min().date().isoformat())
    end = request.args.get("end", raw["datum"].max().date().isoformat())
    metric = request.args.get("metric", "volumen")

    df = filter_data(raw, person, gym, geraete, start, end)
    if metric == "volumen":
        agg = df.groupby(df["datum"].dt.date)["vol_gesamt"].sum().reset_index(name="vol_gesamt")
    elif metric == "gewicht":
        agg = df.groupby(df["datum"].dt.date)["avg_gewicht"].mean().reset_index(name="avg_gewicht")
    elif metric == "wdh":
        agg = df.groupby(df["datum"].dt.date)["avg_wdh"].mean().reset_index(name="avg_wdh")
    else:
        agg = []

    # Plotly
    y_col = agg.columns[1] if len(agg.columns) > 1 else None
    fig = px.line(agg, x=agg.columns[0], y=y_col, title="Metrik über Zeit") if not agg.empty else None
    plot_html = fig.to_html(full_html=False) if fig else ""

    return render_template("gym.html",
                           person=person,
                           gym=gym,
                           metric=metric,
                           table=agg.to_dict(orient="records"),
                           plot_html=plot_html)
