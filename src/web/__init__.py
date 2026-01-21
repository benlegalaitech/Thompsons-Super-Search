"""Flask application factory."""

import os
import json
from flask import Flask
from .config import Config


def create_app():
    """Create and configure the Flask application."""
    app = Flask(
        __name__,
        template_folder='../../templates',
        static_folder='../../static'
    )
    app.config.from_object(Config)

    # Load SOURCE_FOLDER from config.json if not set via environment
    if not app.config.get('SOURCE_FOLDER'):
        config_path = os.path.join(os.path.dirname(__file__), '../../config.json')
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                file_config = json.load(f)
                app.config['SOURCE_FOLDER'] = file_config.get('source_folder', '')

    # Register blueprints
    from .routes import main
    app.register_blueprint(main)

    return app
