"""Admin functionality for user access management."""

import os
from functools import wraps
from flask import abort, request, current_app
import requests

# Configuration from environment
AZURE_AD_CLIENT_ID = os.environ.get('AZURE_AD_CLIENT_ID', '')
AZURE_AD_CLIENT_SECRET = os.environ.get('AZURE_AD_CLIENT_SECRET', '')
AZURE_AD_TENANT_ID = os.environ.get('AZURE_AD_TENANT_ID', '')
SUPER_SEARCH_SP_ID = os.environ.get('SUPER_SEARCH_SP_ID', '')  # Service Principal ID
ADMIN_EMAILS = os.environ.get('ADMIN_EMAILS', '').lower().split(',')

# Graph API endpoints
GRAPH_BASE_URL = 'https://graph.microsoft.com/v1.0'


class GraphAPIError(Exception):
    """Error communicating with Microsoft Graph API."""
    pass


def get_current_user_email():
    """Get the current user's email from Azure AD Easy Auth headers."""
    # Easy Auth passes user info in headers
    # X-MS-CLIENT-PRINCIPAL-NAME contains the email/UPN
    email = request.headers.get('X-MS-CLIENT-PRINCIPAL-NAME', '')
    return email.lower() if email else ''


def get_current_user_name():
    """Get the current user's display name from Azure AD Easy Auth headers."""
    # Try to get from ID token claims
    return request.headers.get('X-MS-CLIENT-PRINCIPAL-NAME', 'Unknown')


def is_admin():
    """Check if the current user is an admin."""
    email = get_current_user_email()
    if not email:
        return False
    return email in [e.strip() for e in ADMIN_EMAILS if e.strip()]


def admin_required(f):
    """Decorator to require admin access."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not is_admin():
            abort(403, 'Admin access required')
        return f(*args, **kwargs)
    return decorated_function


def _get_graph_token():
    """Get an access token for Microsoft Graph API using client credentials."""
    if not all([AZURE_AD_CLIENT_ID, AZURE_AD_CLIENT_SECRET, AZURE_AD_TENANT_ID]):
        raise GraphAPIError('Azure AD configuration missing')

    token_url = f'https://login.microsoftonline.com/{AZURE_AD_TENANT_ID}/oauth2/v2.0/token'

    response = requests.post(token_url, data={
        'client_id': AZURE_AD_CLIENT_ID,
        'client_secret': AZURE_AD_CLIENT_SECRET,
        'scope': 'https://graph.microsoft.com/.default',
        'grant_type': 'client_credentials'
    }, timeout=30)

    if response.status_code != 200:
        raise GraphAPIError(f'Failed to get token: {response.text}')

    return response.json()['access_token']


def _graph_request(method, endpoint, data=None):
    """Make a request to Microsoft Graph API."""
    token = _get_graph_token()
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    url = f'{GRAPH_BASE_URL}{endpoint}'

    if method == 'GET':
        response = requests.get(url, headers=headers, timeout=30)
    elif method == 'POST':
        response = requests.post(url, headers=headers, json=data, timeout=30)
    elif method == 'DELETE':
        response = requests.delete(url, headers=headers, timeout=30)
    else:
        raise ValueError(f'Unsupported method: {method}')

    return response


def get_user_by_email(email: str) -> dict:
    """Look up a user by email address."""
    # Try UPN first (email format)
    response = _graph_request('GET', f'/users/{email}')

    if response.status_code == 200:
        return response.json()

    # Try searching by mail property
    response = _graph_request('GET', f"/users?$filter=mail eq '{email}'")
    if response.status_code == 200:
        users = response.json().get('value', [])
        if users:
            return users[0]

    raise GraphAPIError(f'User not found: {email}')


def list_app_users() -> list:
    """List all users who have access to the app."""
    if not SUPER_SEARCH_SP_ID:
        raise GraphAPIError('Service Principal ID not configured')

    response = _graph_request('GET', f'/servicePrincipals/{SUPER_SEARCH_SP_ID}/appRoleAssignedTo')

    if response.status_code != 200:
        raise GraphAPIError(f'Failed to list users: {response.text}')

    assignments = response.json().get('value', [])

    # Filter to only user assignments (not groups or service principals)
    users = []
    for assignment in assignments:
        if assignment.get('principalType') == 'User':
            users.append({
                'id': assignment.get('principalId'),
                'displayName': assignment.get('principalDisplayName'),
                'assignmentId': assignment.get('id')
            })

    return users


def add_user_access(email: str) -> dict:
    """Grant a user access to the app."""
    if not SUPER_SEARCH_SP_ID:
        raise GraphAPIError('Service Principal ID not configured')

    # Look up the user
    user = get_user_by_email(email)
    user_id = user.get('id')

    if not user_id:
        raise GraphAPIError(f'User ID not found for: {email}')

    # Check if user already has access
    existing_users = list_app_users()
    for existing in existing_users:
        if existing['id'] == user_id:
            return {'status': 'already_exists', 'user': user}

    # Add app role assignment
    response = _graph_request('POST', f'/servicePrincipals/{SUPER_SEARCH_SP_ID}/appRoleAssignments', {
        'principalId': user_id,
        'resourceId': SUPER_SEARCH_SP_ID,
        'appRoleId': '00000000-0000-0000-0000-000000000000'  # Default role
    })

    if response.status_code in [200, 201]:
        return {'status': 'added', 'user': user}

    raise GraphAPIError(f'Failed to add user: {response.text}')


def remove_user_access(assignment_id: str) -> bool:
    """Remove a user's access to the app."""
    if not SUPER_SEARCH_SP_ID:
        raise GraphAPIError('Service Principal ID not configured')

    response = _graph_request('DELETE', f'/servicePrincipals/{SUPER_SEARCH_SP_ID}/appRoleAssignedTo/{assignment_id}')

    if response.status_code in [200, 204]:
        return True

    raise GraphAPIError(f'Failed to remove user: {response.text}')
