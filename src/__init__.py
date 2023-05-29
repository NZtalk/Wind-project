"""Initialize Flask app."""
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from config import Config

db = SQLAlchemy()


def create_app():
    """Construct the core application."""
    app = Flask(__name__, instance_relative_config=False)
    app.config.from_object(Config)

    db.init_app(app)

    with app.app_context():
        #from . import routes  # Import routes

        db.create_all()  # Create database tables for our data models

        return app
    
create_app()