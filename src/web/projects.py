"""Project registry -- loads and validates project definitions from config.json."""

import os
import re
import json

_projects = None  # Cached project list

# Valid project ID pattern: lowercase alphanumeric with hyphens
PROJECT_ID_PATTERN = re.compile(r'^[a-z0-9][a-z0-9-]*$')


def load_projects(config_path=None):
    """Load projects from config.json.

    Handles both the new multi-project format (with 'projects' key)
    and the old flat format (single source_folder / index_folder).
    """
    global _projects
    if _projects is not None:
        return _projects

    if config_path is None:
        config_path = os.path.join(os.path.dirname(__file__), '../../config.json')

    if not os.path.exists(config_path):
        _projects = []
        return _projects

    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    if 'projects' in config:
        _projects = config['projects']
    else:
        # Backward compat: convert flat config to single project
        _projects = [{
            'id': 'default',
            'name': 'Document Search',
            'description': '',
            'source_folder': config.get('source_folder', ''),
            'excel_source_folder': config.get('excel_source_folder', ''),
            'index_folder': config.get('index_folder', './index'),
            'file_extensions': config.get('file_extensions', ['.pdf']),
            'excel_extensions': config.get('excel_extensions', ['.xls', '.xlsx', '.xlsm', '.xlsb']),
        }]

    return _projects


def get_project(project_id):
    """Get a project by ID. Returns None if not found."""
    if not PROJECT_ID_PATTERN.match(project_id):
        return None
    projects = load_projects()
    for p in projects:
        if p['id'] == project_id:
            return p
    return None


def get_all_projects():
    """Get list of all projects with public-safe fields for the picker UI."""
    projects = load_projects()
    return [{
        'id': p['id'],
        'name': p['name'],
        'description': p.get('description', '')
    } for p in projects]


def reload_projects():
    """Force reload projects from disk (useful after config changes)."""
    global _projects
    _projects = None
    return load_projects()
