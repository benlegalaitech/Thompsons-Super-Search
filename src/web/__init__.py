"""Flask application factory."""

import os
import sys
from flask import Flask
from .config import Config


def get_index_folder(project_id: str, config_index_folder: str = None) -> str:
    """Get the index folder path for a project.

    Uses mounted storage path if INDEX_MOUNT_PATH is set, otherwise falls back to config.
    """
    mount_path = os.environ.get('INDEX_MOUNT_PATH', '')
    if mount_path and os.path.isdir(mount_path):
        return os.path.join(mount_path, project_id)
    return config_index_folder or f'./index/{project_id}'


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

    # Start background index download for all projects (non-blocking)
    try:
        from .blob_storage import is_blob_storage_enabled, start_background_index_download
        mount_path = os.environ.get('INDEX_MOUNT_PATH', '')
        if mount_path:
            print(f"Using mounted index storage at: {mount_path}", file=sys.stderr, flush=True)

        if is_blob_storage_enabled() and projects:
            for project in projects:
                project_id = project['id']
                config_folder = project.get('index_folder', f'./index/{project_id}')
                index_folder = get_index_folder(project_id, config_folder)
                print(f"Blob storage enabled, starting background download for project '{project_id}' to {index_folder}...", file=sys.stderr, flush=True)
                start_background_index_download(index_folder, project_id=project_id)
        elif not is_blob_storage_enabled():
            print("Blob storage not enabled", file=sys.stderr, flush=True)
    except Exception as e:
        print(f"Error starting background download: {e}", file=sys.stderr, flush=True)

    print("App creation complete", file=sys.stderr, flush=True)
    return app
