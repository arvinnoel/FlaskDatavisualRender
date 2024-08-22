from flask import Flask
from flask_cors import CORS
from routes.routes import routes_bp  # Import the Blueprint

app = Flask(__name__)

# Enable CORS for all routes, specifying origins if needed
CORS(app, resources={r"/*": {"origins": "http://localhost:4200"}})

# Register the Blueprint
app.register_blueprint(routes_bp)
