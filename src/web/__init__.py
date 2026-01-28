"""Flask application factory."""

import os
import sys
from flask import Flask
from .config import Config


def create_app():
    """Create and configure the Flask application."""
    print("Creating Flask application...", file=sys.stderr, flush=True)

    app = Flask(
        __name__,
        template_folder='../../templates',
        static_folder='../../static'
    )
    app.config.from_object(Config)
    print("Flask config loaded", file=sys.stderr, flush=True)

    # Load project definitions from config.json
    from .projects import load_projects
    config_path = os.path.join(os.path.dirname(__file__), '../../config.json')
    projects = load_projects(config_path)
    app.config['PROJECTS'] = projects
    print(f"Loaded {len(projects)} project(s)", file=sys.stderr, flush=True)

    # Register blueprints
    from .routes import main
    app.register_blueprint(main)
    print("Blueprints registered", file=sys.stderr, flush=True)

    # Start background index download (non-blocking)
    try:
        from .blob_storage import is_blob_storage_enabled, start_background_index_download
        if is_blob_storage_enabled() and projects:
            first_project = projects[0]
            project_id = first_project['id']
            index_folder = first_project.get('index_folder', f'./index/{project_id}')
            print(f"Blob storage enabled, starting background download for project '{project_id}'...", file=sys.stderr, flush=True)
            start_background_index_download(index_folder, project_id=project_id)
        elif not is_blob_storage_enabled():
            print("Blob storage not enabled", file=sys.stderr, flush=True)
    except Exception as e:
        print(f"Error starting background download: {e}", file=sys.stderr, flush=True)

    print("App creation complete", file=sys.stderr, flush=True)
    return app
