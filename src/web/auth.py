"""Simple password authentication for web interface."""

import os
from functools import wraps
from flask import session, redirect, url_for, request, current_app


def is_azure_ad_authenticated():
    """Check if user is authenticated via Azure AD Easy Auth."""
    # Easy Auth passes user info in X-MS-CLIENT-PRINCIPAL-NAME header
    principal_name = request.headers.get('X-MS-CLIENT-PRINCIPAL-NAME', '')
    return bool(principal_name)


def get_azure_ad_user():
    """Get the current user's email from Azure AD Easy Auth."""
    return request.headers.get('X-MS-CLIENT-PRINCIPAL-NAME', '')


def login_required(f):
    """Decorator to require authentication.

    Supports both Azure AD Easy Auth and session-based auth.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check Azure AD Easy Auth first
        if is_azure_ad_authenticated():
            return f(*args, **kwargs)
        # Fall back to session-based auth
        if session.get('authenticated'):
            return f(*args, **kwargs)
        return redirect(url_for('main.login', next=request.url))
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
