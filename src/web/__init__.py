"""Flask application factory."""

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

    # Register blueprints
    from .routes import main
    app.register_blueprint(main)

    return app
