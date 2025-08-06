from flask import Flask
from dashboards.routes import gym_bp

app = Flask(__name__)
app.register_blueprint(gym_bp)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000)
