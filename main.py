from flask import Flask
from flask_cors import CORS
from routes.routes import routes_bp  # Import the Blueprint
from waitress import serve

app = Flask(__name__)

# Enable CORS for all routes, specifying origins if needed
CORS(app, resources={r"/*": {"origins": "http://localhost:4200"}})

# Register the Blueprint
app.register_blueprint(routes_bp)

if __name__ == '__main__':
    # Bind to 0.0.0.0 to make the app accessible externally
    serve(app, host='0.0.0.0', port=8080)
