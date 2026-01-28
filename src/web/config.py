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

    # Default index location (used as fallback; per-project index_folder takes priority)
    INDEX_FOLDER = os.environ.get('INDEX_FOLDER', './index')

    # Azure Blob Storage (optional - if configured, PDFs are served from blob)
    AZURE_STORAGE_ACCOUNT = os.environ.get('AZURE_STORAGE_ACCOUNT', '')
    AZURE_STORAGE_KEY = os.environ.get('AZURE_STORAGE_KEY', '')
    AZURE_PDF_CONTAINER = os.environ.get('AZURE_PDF_CONTAINER', 'pdfs')
    AZURE_INDEX_CONTAINER = os.environ.get('AZURE_INDEX_CONTAINER', 'index')
