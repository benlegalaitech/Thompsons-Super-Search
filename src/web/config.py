"""Flask configuration."""

import os
from datetime import timedelta


class Config:
    """Application configuration."""

    # Flask
    SECRET_KEY = os.environ.get('FLASK_SECRET_KEY', 'dev-secret-key-change-in-production')

    # Authentication
    APP_PASSWORD = os.environ.get('APP_PASSWORD', '')

    # Session
    SESSION_TYPE = 'filesystem'
    PERMANENT_SESSION_LIFETIME = timedelta(hours=1)

    # Index location
    INDEX_FOLDER = os.environ.get('INDEX_FOLDER', './index')
