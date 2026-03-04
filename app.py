from flask import Flask, jsonify
from blueprints.Users.users import users_bp
from blueprints.Cows.cows import cows_bp
from blueprints.Health_Records.health_records import health_bp
from blueprints.Breeding_records.breeding_records import breeding_bp
from blueprints.Audit_Log.audit_log import audit_bp
from blueprints.Dashboard.dashboard import dashboard_bp
from blueprints.Locations.locations import locations_bp
from blueprints.Auth.auth import auth_bp

from db import get_conn

from flask_cors import CORS

import os
from dotenv import load_dotenv
load_dotenv()

def create_app():
    app = Flask(__name__)

    CORS(app, supports_credentials=True, origins=["http://localhost:4200", "https://ctims-frontend-james-hsbeddbed4ced8cv.uksouth-01.azurewebsites.net"])
    app.config['SESSION_COOKIE_SAMESITE'] = 'None' 
    app.config['SESSION_COOKIE_SECURE'] = True

    app.secret_key = os.getenv("SECRET_KEY", "dev-secret")

    app.config["SECRET_KEY"] = "dev-secret-key" 

    app.register_blueprint(users_bp, url_prefix="/api/users")
    app.register_blueprint(cows_bp, url_prefix="/api/cows")
    app.register_blueprint(health_bp, url_prefix="/api/health")
    app.register_blueprint(breeding_bp, url_prefix="/api/breeding")
    app.register_blueprint(audit_bp, url_prefix="/api/audit")
    app.register_blueprint(dashboard_bp, url_prefix="/api/dashboard")
    app.register_blueprint(locations_bp, url_prefix="/api/locations")
    app.register_blueprint(auth_bp, url_prefix="/api/auth")

    @app.route("/api/healthcheck")
    def healthcheck():
        return jsonify({"status": "ok", "message": "CTIMS backend running"})
    
    @app.route("/")
    def home():
        return jsonify({
            "message": "CTIMS backend running",
            "try": [
                "/api/healthcheck",
                "/api/users",
                "/api/cows"
            ]
        })

    @app.route("/api/dbcheck")
    def dbcheck():
        try:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("SELECT DB_NAME()")
            db_name = cur.fetchone()[0]
            conn.close()
            return jsonify({"db": "ok", "database": db_name})
        except Exception as e:
            return jsonify({"db": "error", "details": str(e)}), 500
        

    

    return app


app = create_app()

if __name__ == "__main__":
    app.run(debug=True)
