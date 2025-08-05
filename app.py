from flask import Flask
from dashboards.routes import gym_bp

app = Flask(__name__)
app.register_blueprint(gym_bp, url_prefix="/gym")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
