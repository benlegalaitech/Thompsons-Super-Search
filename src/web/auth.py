"""Simple password authentication for web interface."""

from functools import wraps
from flask import session, redirect, url_for, request, current_app


def login_required(f):
    """Decorator to require authentication."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('authenticated'):
            return redirect(url_for('main.login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function


def check_password(password: str) -> bool:
    """Check if password matches the configured app password."""
    app_password = current_app.config.get('APP_PASSWORD', '')
    if not app_password:
        # No password configured - allow access (for development)
        return True
    return password == app_password


def authenticate_user():
    """Mark user as authenticated in session."""
    session['authenticated'] = True
    session.permanent = True


def logout_user():
    """Clear authentication from session."""
    session.pop('authenticated', None)
