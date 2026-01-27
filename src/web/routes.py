"""Search routes and logic."""

import os
import json
import re
from markupsafe import Markup
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, current_app, send_file, abort
from .auth import login_required, check_password, authenticate_user, logout_user
from .blob_storage import is_blob_storage_enabled, generate_pdf_sas_url, check_blob_exists, download_index_from_blob, is_index_download_complete, is_index_downloading, get_blob_service_client, PDF_CONTAINER

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

    # If blob storage is enabled and index is still downloading, wait
    if is_blob_storage_enabled() and is_index_downloading() and not is_index_download_complete():
        # Background download in progress, return loading state
        return [], {'total_docs': 0, 'total_pages': 0, 'loading': True}

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
                        # Default file_type for legacy PDF-only index files
                        if 'file_type' not in doc:
                            doc['file_type'] = 'pdf'
                        _index.append(doc)
                except Exception as e:
                    print(f"Error loading {filepath}: {e}")

    _metadata['total_docs'] = len(_index)
    _metadata['total_pages'] = sum(len(doc.get('pages', [])) for doc in _index)

    return _index, _metadata


def parse_search_terms(query):
    """Parse search query into individual terms.

    Supports:
    - Multiple words: 'Ford Confidential' -> ['ford', 'confidential'] (AND search)
    - Quoted phrases: '"exact phrase"' -> ['exact phrase'] (exact match)
    """
    terms = []
    query = query.strip()

    # Extract quoted phrases first
    quoted_pattern = r'"([^"]+)"'
    quoted_matches = re.findall(quoted_pattern, query)
    terms.extend([m.lower() for m in quoted_matches])

    # Remove quoted phrases from query
    remaining = re.sub(quoted_pattern, '', query).strip()

    # Split remaining by whitespace
    if remaining:
        words = remaining.split()
        terms.extend([w.lower() for w in words if w])

    return terms


def search_index(query, page=1, per_page=20, file_type_filter=None):
    """Search the index for matching documents.

    Smart search: Multiple words are treated as AND search.
    Use quotes for exact phrase matching: "Ford Confidential"
    file_type_filter: None (all), 'pdf', or 'excel'
    """
    index, metadata = load_index()

    if not query or not query.strip():
        return {
            'query': '',
            'total_matches': 0,
            'documents': 0,
            'results': [],
            'page': page,
            'total_pages': 0,
            'has_more': False
        }

    search_terms = parse_search_terms(query)
    if not search_terms:
        return {
            'query': query,
            'total_matches': 0,
            'documents': 0,
            'results': [],
            'page': page,
            'total_pages': 0,
            'has_more': False
        }

    results = []
    seen_docs = set()

    for doc in index:
        # Apply file type filter
        doc_type = doc.get('file_type', 'pdf')
        if file_type_filter and doc_type != file_type_filter:
            continue
        filename = doc.get('filename', '')
        filepath = doc.get('path', '')

        for page_info in doc.get('pages', []):
            page_num = page_info.get('page_num', 0)
            text = page_info.get('text', '')
            text_lower = text.lower()

            # Check if ALL search terms are present (AND search)
            if all(term in text_lower for term in search_terms):
                # Extract context around the first matching term
                context = extract_context(text, search_terms[0])

                # Count total matches across all terms
                match_count = sum(text_lower.count(term) for term in search_terms)

                results.append({
                    'filename': filename,
                    'filepath': filepath,
                    'page': page_num,
                    'sheet_name': page_info.get('sheet_name', ''),
                    'file_type': doc.get('file_type', 'pdf'),
                    'context': context,
                    'match_count': match_count
                })
                seen_docs.add(filename)

    # Sort by match count (most matches first)
    results.sort(key=lambda x: x['match_count'], reverse=True)

    # Pagination
    total_matches = len(results)
    total_pages = (total_matches + per_page - 1) // per_page  # Ceiling division
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    paginated_results = results[start_idx:end_idx]

    return {
        'query': query,
        'total_matches': total_matches,
        'documents': len(seen_docs),
        'results': paginated_results,
        'page': page,
        'total_pages': total_pages,
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


def highlight_matches(text, search_terms):
    """Highlight all search terms in text (case-insensitive)."""
    if not search_terms:
        return text

    for term in search_terms:
        pattern = re.compile(re.escape(term), re.IGNORECASE)
        text = pattern.sub(lambda m: f'<mark>{m.group()}</mark>', text)

    return text


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
    file_type = request.args.get('type', '')  # '', 'pdf', or 'excel'

    results = search_index(query, page=page, file_type_filter=file_type or None)

    # Highlight matches in context (using parsed search terms)
    search_terms = parse_search_terms(query)
    for result in results['results']:
        result['context'] = highlight_matches(result['context'], search_terms)

    return jsonify(results)


@main.route('/api/stats')
@login_required
def api_stats():
    """Get index statistics."""
    _, metadata = load_index()
    return jsonify(metadata)


@main.route('/health')
def health():
    """Health check endpoint for container orchestration."""
    return jsonify({
        'status': 'ok',
        'index_downloading': is_index_downloading(),
        'index_ready': is_index_download_complete()
    })


@main.route('/pdf/<path:filepath>')
@login_required
def serve_pdf(filepath):
    """Serve a PDF file - from blob storage or local source folder."""

    # If blob storage is configured, redirect to SAS URL
    if is_blob_storage_enabled():
        current_app.logger.info(f"PDF request (blob): filepath={filepath!r}")

        if not check_blob_exists(filepath):
            current_app.logger.error(f"File not found in blob storage: {filepath}")
            abort(404, 'File not found in blob storage')

        sas_url = generate_pdf_sas_url(filepath, expiry_hours=1)
        return redirect(sas_url)

    # Fallback to local file serving (original logic)
    source_folder = current_app.config.get('SOURCE_FOLDER', '')
    current_app.logger.info(f"PDF request (local): filepath={filepath!r}, source_folder={source_folder!r}")

    if not source_folder:
        current_app.logger.error("SOURCE_FOLDER not configured")
        abort(404, 'Source folder not configured')

    # Construct full path and ensure it's within source folder (security)
    full_path = os.path.normpath(os.path.join(source_folder, filepath))
    current_app.logger.info(f"Full path: {full_path!r}, exists={os.path.exists(full_path)}")

    if not full_path.startswith(os.path.normpath(source_folder)):
        abort(403, 'Access denied')

    if not os.path.exists(full_path):
        current_app.logger.error(f"File not found: {full_path}")
        abort(404, 'File not found')

    return send_file(full_path, mimetype='application/pdf')


def _resolve_excel_path(filepath):
    """Resolve an Excel filepath to a local file path, with security checks.

    If blob storage is enabled, downloads the file to a temp location first.
    Returns the local file path (either from EXCEL_SOURCE_FOLDER or temp download).
    """
    # If blob storage is enabled, download from blob to temp file
    if is_blob_storage_enabled():
        if not check_blob_exists(filepath):
            current_app.logger.error(f"Excel file not found in blob storage: {filepath}")
            abort(404, 'File not found in blob storage')

        import tempfile
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(PDF_CONTAINER, filepath)

        # Download to temp file preserving the extension
        ext = os.path.splitext(filepath)[1]
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=ext)
        try:
            tmp.write(blob_client.download_blob().readall())
            tmp.close()
            return tmp.name
        except Exception as e:
            tmp.close()
            os.unlink(tmp.name)
            current_app.logger.error(f"Error downloading Excel from blob: {e}")
            abort(500, 'Error downloading file from storage')

    # Local file serving
    excel_source = current_app.config.get('EXCEL_SOURCE_FOLDER', '')

    if not excel_source:
        current_app.logger.error("EXCEL_SOURCE_FOLDER not configured")
        abort(404, 'Excel source folder not configured')

    full_path = os.path.normpath(os.path.join(excel_source, filepath))

    # Security: ensure path is within source folder
    if not full_path.startswith(os.path.normpath(excel_source)):
        abort(403, 'Access denied')

    if not os.path.exists(full_path):
        current_app.logger.error(f"Excel file not found: {full_path}")
        abort(404, 'File not found')

    return full_path


def _open_excel_workbook(full_path):
    """Open an Excel file and return (sheets_data, sheet_names).

    sheets_data is a dict: {sheet_name: list of rows}, where each row is a list of cell values.
    """
    ext = os.path.splitext(full_path)[1].lower()
    sheets_data = {}
    sheet_names = []

    if ext in ('.xlsx', '.xlsm'):
        import openpyxl
        from openpyxl.chartsheet import Chartsheet
        wb = openpyxl.load_workbook(full_path, read_only=True, data_only=True)
        try:
            sheet_names = wb.sheetnames
            for sname in sheet_names:
                ws = wb[sname]
                if isinstance(ws, Chartsheet):
                    continue
                rows = []
                for row in ws.iter_rows(values_only=True):
                    rows.append([c for c in row])
                sheets_data[sname] = rows
        finally:
            wb.close()

    elif ext == '.xls':
        import xlrd
        wb = xlrd.open_workbook(full_path)
        sheet_names = wb.sheet_names()
        for sname in sheet_names:
            ws = wb.sheet_by_name(sname)
            rows = []
            for r in range(ws.nrows):
                row = [ws.cell_value(r, c) for c in range(ws.ncols)]
                rows.append(row)
            sheets_data[sname] = rows

    elif ext == '.xlsb':
        from pyxlsb import open_workbook
        with open_workbook(full_path) as wb:
            sheet_names = wb.sheets
            for sname in sheet_names:
                with wb.get_sheet(sname) as ws:
                    rows = []
                    for row in ws.rows():
                        rows.append([c.v for c in row])
                    sheets_data[sname] = rows
    else:
        abort(400, f'Unsupported file type: {ext}')

    return sheets_data, sheet_names


def _render_sheet_html(rows, search_terms, full_mode=False, context_rows=5):
    """Render sheet rows as an HTML table with highlighted matches.

    In contextual mode (full_mode=False): shows header + rows around matches.
    In full mode: shows all rows.
    Returns (html_string, match_count).
    """
    if not rows:
        return '<p class="no-data">This sheet is empty.</p>', 0

    # Detect header row
    first_row = rows[0]
    has_header = False
    non_empty = [c for c in first_row if c is not None and str(c).strip()]
    if non_empty:
        string_count = sum(1 for c in non_empty if isinstance(c, str))
        has_header = string_count / len(non_empty) >= 0.6 if non_empty else False

    header_row = rows[0] if has_header else None
    data_rows = rows[1:] if has_header else rows

    if full_mode:
        # Show all rows
        visible_indices = set(range(len(data_rows)))
    else:
        # Find rows containing any search term
        match_indices = []
        for i, row in enumerate(data_rows):
            row_text = ' '.join(str(c) for c in row if c is not None).lower()
            if any(term in row_text for term in search_terms):
                match_indices.append(i)

        # Build visible set: context_rows before and after each match
        visible_indices = set()
        for idx in match_indices:
            for offset in range(-context_rows, context_rows + 1):
                adj = idx + offset
                if 0 <= adj < len(data_rows):
                    visible_indices.add(adj)

        # If no matches found in data rows, show first 20 rows
        if not visible_indices:
            visible_indices = set(range(min(20, len(data_rows))))

    # Build HTML table
    parts = ['<table class="excel-table">']

    # Header row
    if header_row:
        parts.append('<thead><tr>')
        for cell in header_row:
            cell_str = str(cell) if cell is not None else ''
            cell_html = _escape_html(cell_str)
            if search_terms:
                cell_html = highlight_matches(cell_html, search_terms)
            parts.append(f'<th>{cell_html}</th>')
        parts.append('</tr></thead>')

    # Data rows
    parts.append('<tbody>')
    match_count = 0
    prev_idx = -2  # Track gaps for dividers
    sorted_indices = sorted(visible_indices)

    for idx in sorted_indices:
        # Insert divider if there's a gap
        if idx > prev_idx + 1 and prev_idx >= 0:
            col_count = len(header_row) if header_row else len(data_rows[idx]) if data_rows[idx] else 1
            parts.append(f'<tr class="match-divider"><td colspan="{col_count}">...</td></tr>')

        row = data_rows[idx]
        row_text = ' '.join(str(c) for c in row if c is not None).lower()
        is_match = any(term in row_text for term in search_terms)
        row_class = ' class="match-row"' if is_match else ''

        if is_match:
            match_count += 1

        parts.append(f'<tr{row_class}>')
        for cell in row:
            cell_str = str(cell) if cell is not None else ''
            if cell_str == 'None':
                cell_str = ''
            cell_html = _escape_html(cell_str)
            if search_terms and cell_str:
                cell_html = highlight_matches(cell_html, search_terms)
            parts.append(f'<td>{cell_html}</td>')
        parts.append('</tr>')
        prev_idx = idx

    parts.append('</tbody></table>')

    total_rows = len(data_rows)
    shown_rows = len(sorted_indices)
    if not full_mode and shown_rows < total_rows:
        parts.append(f'<p class="truncation-notice">Showing {shown_rows} of {total_rows} rows (rows near matches). <a href="?full=1&sheet={{}}&q={{}}" class="view-full-link">View entire sheet</a></p>')

    return '\n'.join(parts), match_count


def _escape_html(text):
    """Escape HTML special characters."""
    return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')


@main.route('/excel-view/<path:filepath>')
@login_required
def excel_view(filepath):
    """View an Excel sheet as an HTML table with search term highlighting."""
    sheet_name = request.args.get('sheet', '')
    query = request.args.get('q', '')
    full_mode = request.args.get('full', '0') == '1'

    full_path = _resolve_excel_path(filepath)
    is_temp = is_blob_storage_enabled()

    try:
        sheets_data, sheet_names = _open_excel_workbook(full_path)
    finally:
        # Clean up temp file from blob download
        if is_temp and os.path.exists(full_path):
            os.unlink(full_path)

    # Default to first sheet if not specified or invalid
    if not sheet_name or sheet_name not in sheets_data:
        sheet_name = sheet_names[0] if sheet_names else ''

    search_terms = parse_search_terms(query) if query else []

    # Render the requested sheet
    rows = sheets_data.get(sheet_name, [])
    table_html, match_count = _render_sheet_html(rows, search_terms, full_mode=full_mode)

    # Fix the view-full-link placeholders
    from urllib.parse import quote
    table_html = table_html.replace(
        'sheet={}&q={}',
        f'sheet={quote(sheet_name)}&q={quote(query)}'
    )

    return render_template('excel_view.html',
        filename=os.path.basename(filepath),
        filepath=filepath,
        sheet_names=sheet_names,
        active_sheet=sheet_name,
        query=query,
        full_mode=full_mode,
        match_count=match_count,
        table_html=Markup(table_html),
        total_rows=len(rows)
    )


EXCEL_MIME_TYPES = {
    '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    '.xls': 'application/vnd.ms-excel',
    '.xlsm': 'application/vnd.ms-excel.sheet.macroEnabled.12',
    '.xlsb': 'application/vnd.ms-excel.sheet.binary.macroEnabled.12',
}


@main.route('/file/<path:filepath>')
@login_required
def serve_file(filepath):
    """Serve an Excel file for download."""
    ext = os.path.splitext(filepath)[1].lower()

    # For blob storage, generate SAS URL
    if is_blob_storage_enabled():
        if not check_blob_exists(filepath):
            abort(404, 'File not found in blob storage')
        sas_url = generate_pdf_sas_url(filepath, expiry_hours=1)
        return redirect(sas_url)

    # Local file serving
    full_path = _resolve_excel_path(filepath)

    mimetype = EXCEL_MIME_TYPES.get(ext, 'application/octet-stream')
    return send_file(full_path, mimetype=mimetype,
                     as_attachment=True,
                     download_name=os.path.basename(filepath))
