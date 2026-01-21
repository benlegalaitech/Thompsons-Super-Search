"""Search routes and logic."""

import os
import json
import re
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, current_app
from .auth import login_required, check_password, authenticate_user, logout_user

main = Blueprint('main', __name__)

# Global index storage (loaded on first request)
_index = None
_metadata = None


def get_index_folder():
    """Get the index folder path."""
    return current_app.config.get('INDEX_FOLDER', './index')


def load_index():
    """Load all extracted text into memory."""
    global _index, _metadata

    if _index is not None:
        return _index, _metadata

    index_folder = get_index_folder()
    texts_folder = os.path.join(index_folder, 'texts')
    metadata_file = os.path.join(index_folder, 'metadata.json')

    _index = []
    _metadata = {'total_docs': 0, 'total_pages': 0}

    # Load metadata if exists
    if os.path.exists(metadata_file):
        with open(metadata_file, 'r', encoding='utf-8') as f:
            _metadata = json.load(f)

    # Load all text files
    if os.path.exists(texts_folder):
        for filename in os.listdir(texts_folder):
            if filename.endswith('.json'):
                filepath = os.path.join(texts_folder, filename)
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        doc = json.load(f)
                        _index.append(doc)
                except Exception as e:
                    print(f"Error loading {filepath}: {e}")

    _metadata['total_docs'] = len(_index)
    _metadata['total_pages'] = sum(len(doc.get('pages', [])) for doc in _index)

    return _index, _metadata


def search_index(query, page=1, per_page=20):
    """Search the index for matching documents."""
    index, metadata = load_index()

    if not query or not query.strip():
        return {
            'query': '',
            'total_matches': 0,
            'documents': 0,
            'results': [],
            'page': page,
            'has_more': False
        }

    query_lower = query.lower().strip()
    results = []
    seen_docs = set()

    for doc in index:
        filename = doc.get('filename', '')
        filepath = doc.get('path', '')

        for page_info in doc.get('pages', []):
            page_num = page_info.get('page_num', 0)
            text = page_info.get('text', '')

            if query_lower in text.lower():
                # Extract context around the match
                context = extract_context(text, query_lower)
                match_count = text.lower().count(query_lower)

                results.append({
                    'filename': filename,
                    'filepath': filepath,
                    'page': page_num,
                    'context': context,
                    'match_count': match_count
                })
                seen_docs.add(filename)

    # Sort by match count (most matches first)
    results.sort(key=lambda x: x['match_count'], reverse=True)

    # Pagination
    total_matches = len(results)
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    paginated_results = results[start_idx:end_idx]

    return {
        'query': query,
        'total_matches': total_matches,
        'documents': len(seen_docs),
        'results': paginated_results,
        'page': page,
        'has_more': end_idx < total_matches
    }


def extract_context(text, query, context_chars=100):
    """Extract a snippet of text around the first match."""
    text_lower = text.lower()
    pos = text_lower.find(query)

    if pos == -1:
        return text[:200] + '...' if len(text) > 200 else text

    # Get context before and after
    start = max(0, pos - context_chars)
    end = min(len(text), pos + len(query) + context_chars)

    snippet = text[start:end]

    # Add ellipsis if truncated
    if start > 0:
        snippet = '...' + snippet
    if end < len(text):
        snippet = snippet + '...'

    return snippet


def highlight_matches(text, query):
    """Highlight query matches in text (case-insensitive)."""
    if not query:
        return text

    pattern = re.compile(re.escape(query), re.IGNORECASE)
    return pattern.sub(lambda m: f'<mark>{m.group()}</mark>', text)


# Routes

@main.route('/login', methods=['GET', 'POST'])
def login():
    """Login page."""
    if request.method == 'POST':
        password = request.form.get('password', '')
        if check_password(password):
            authenticate_user()
            next_url = request.args.get('next', url_for('main.index'))
            return redirect(next_url)
        flash('Invalid password', 'error')

    return render_template('login.html')


@main.route('/logout')
def logout():
    """Logout and redirect to login."""
    logout_user()
    return redirect(url_for('main.login'))


@main.route('/')
@login_required
def index():
    """Main search page."""
    _, metadata = load_index()
    return render_template('search.html', metadata=metadata)


@main.route('/api/search')
@login_required
def api_search():
    """Search API endpoint."""
    query = request.args.get('q', '')
    page = request.args.get('page', 1, type=int)

    results = search_index(query, page=page)

    # Highlight matches in context
    for result in results['results']:
        result['context'] = highlight_matches(result['context'], query)

    return jsonify(results)


@main.route('/api/stats')
@login_required
def api_stats():
    """Get index statistics."""
    _, metadata = load_index()
    return jsonify(metadata)
