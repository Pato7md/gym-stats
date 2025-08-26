from flask import Flask, request, jsonify
from rag_service.embeddings import embed_text
from rag_service.db import init_collection, search_chunks
from sqlalchemy import text
from db import ENGINE
from openai import OpenAI
import os

# OpenAI Client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

app = Flask(__name__)
init_collection()

# --------------------------------
# Bestehende Endpoints (search, users, gym_log)
# --------------------------------
@app.route("/search", methods=["POST"])
def search():
    query = request.json.get("query")
    if not query:
        return jsonify({"error": "Bitte 'query' im JSON-Body angeben"}), 400

    query_vector = embed_text(query)
    results = search_chunks(query_vector, limit=3)

    return jsonify({
        "query": query,
        "results": [
            {
                "id": results["ids"][0][i],
                "text": results["documents"][0][i],
                "filepath": results["metadatas"][0][i]["filepath"],
                "score": results["distances"][0][i]
            }
            for i in range(len(results["ids"][0]))
        ]
    })


@app.route("/users", methods=["GET"])
def get_users():
    with ENGINE.connect() as conn:
        result = conn.execute(text("SELECT name FROM persons"))
        users = [row[0] for row in result]
    return jsonify({"users": users})


@app.route("/gym_log", methods=["GET"])
def get_gym_log():
    with ENGINE.connect() as conn:
        result = conn.execute(text("""
            SELECT g.id, p.name AS person, g.gym, g.gerät, g.sätze,
                   g.satz1_gew, g.satz1_wdh,
                   g.satz2_gew, g.satz2_wdh,
                   g.satz3_gew, g.satz3_wdh,
                   g.timestamp
            FROM gym_log g
            LEFT JOIN persons p ON g.person_id = p.id
            ORDER BY g.timestamp DESC
        """))
        logs = [
            {
                "id": row[0],
                "person": row[1],
                "gym": row[2],
                "gerät": row[3],
                "sätze": row[4],
                "satz1": {"gewicht": row[5], "wdh": row[6]},
                "satz2": {"gewicht": row[7], "wdh": row[8]},
                "satz3": {"gewicht": row[9], "wdh": row[10]},
                "timestamp": row[11].isoformat() if row[11] else None,
            }
            for row in result
        ]
    return jsonify({"gym_log": logs})

# --------------------------------
# NEU: /ask Endpoint (smarte RAG)
# --------------------------------
@app.route("/ask", methods=["POST"])
def ask():
    data = request.get_json()
    frage = data.get("question", "")

    if not frage:
        return jsonify({"error": "Bitte eine 'question' im JSON-Body senden"}), 400

    # DB-Schema für den Prompt
    schema = """
    Tabellen und Spalten in der Datenbank:
    - persons(id, name, created_at)
    - gym_log(id, person_id [FK → persons.id], gym, gerät, sätze,
              satz1_gew, satz1_wdh,
              satz2_gew, satz2_wdh,
              satz3_gew, satz3_wdh,
              timestamp)
    Hinweis: 
    - gym_log.person_id verweist auf persons.id.
    - Wenn nach einem Namen gefragt wird, immer persons.name nutzen.
    - Vergleiche von Namen IMMER mit LOWER(...)=LOWER(...) machen.
    
    Hinweis: Vergleiche von Namen sollten immer case-insensitiv erfolgen mit LOWER(p.name) = LOWER('...').
    """

    # Schritt 1: SQL von GPT erzeugen lassen
    sql_prompt = [
        {"role": "system", "content": "Du bist ein SQL-Experte. Erzeuge nur gültige PostgreSQL-SELECT-Statements. Keine INSERT, UPDATE, DELETE, DROP oder ALTER!"},
        {"role": "user", "content": f"Frage: {frage}\n\n{schema}\n\nErzeuge nur das SQL (keine Erklärung)."}
    ]

    sql_response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=sql_prompt
    )
    sql_query = sql_response.choices[0].message.content.strip()

    # Falls GPT Codeblöcke (```sql ... ```) zurückgibt → rausfiltern
    if sql_query.startswith("```"):
        sql_query = sql_query.strip("`")        # Entfernt Backticks
        sql_query = sql_query.replace("sql\n", "", 1).replace("sql", "", 1).strip()

    # Sicherheit: nur SELECT erlauben
    if not sql_query.lower().startswith("select"):
        return jsonify({"error": "Ungültiger SQL-Query (nur SELECT erlaubt)", "sql": sql_query}), 400

    # Schritt 2: SQL ausführen
    with ENGINE.connect() as conn:
        result = conn.execute(text(sql_query))
        rows = [dict(row._mapping) for row in result]

    # Schritt 3: Ergebnis in natürlicher Sprache von GPT erklären lassen
    answer_prompt = [
        {"role": "system", "content": "Du bist ein Assistent, der SQL-Ergebnisse in normale Sprache erklärt."},
        {"role": "user", "content": f"Frage: {frage}\n\nSQL: {sql_query}\n\nErgebnisse:\n{rows}\n\nErkläre die Antwort kurz und klar."}
    ]

    answer_response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=answer_prompt
    )
    answer = answer_response.choices[0].message.content

    return jsonify({"question": frage, "sql": sql_query, "rows": rows, "answer": answer})
