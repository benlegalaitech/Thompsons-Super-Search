#!/usr/bin/env python3
"""
HTML Text Extraction Script

Extracts text from HTML files and saves as JSON for searching.
Produces the same index format as extract.py (for PDFs).

Usage:
    python extract_html.py --project lung-disease
    python extract_html.py --source /path
    python extract_html.py --reindex
"""

import os
import sys
import json
import argparse
from datetime import datetime
from pathlib import Path

from tqdm import tqdm

HTML_EXTENSIONS = ['.html', '.htm']


def load_config():
    """Load configuration from config.json."""
    config_path = Path('config.json')
    if not config_path.exists():
        print("Error: config.json not found.")
        sys.exit(1)

    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def find_html_files(source_folder, extensions=None):
    """Find all HTML files in the source folder recursively."""
    if extensions is None:
        extensions = HTML_EXTENSIONS

    files = []
    source_path = Path(source_folder)

    if not source_path.exists():
        print(f"Error: Source folder does not exist: {source_folder}")
        sys.exit(1)

    for ext in extensions:
        files.extend(source_path.rglob(f'*{ext}'))
        files.extend(source_path.rglob(f'*{ext.upper()}'))

    return sorted(set(files))


def extract_text_from_html(filepath):
    """Extract text content from HTML file using BeautifulSoup."""
    try:
        from bs4 import BeautifulSoup

        # Try different encodings
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        content = None

        for encoding in encodings:
            try:
                with open(filepath, 'r', encoding=encoding, errors='ignore') as f:
                    content = f.read()
                break
            except (UnicodeDecodeError, LookupError):
                continue

        if content is None:
            # Last resort: read as binary and decode
            with open(filepath, 'rb') as f:
                content = f.read().decode('utf-8', errors='ignore')

        # Parse with BeautifulSoup
        soup = BeautifulSoup(content, 'html.parser')

        # Remove script, style, nav, footer, header elements
        for element in soup(['script', 'style', 'nav', 'footer', 'header', 'noscript', 'meta', 'link']):
            element.decompose()

        # Get text with whitespace normalized
        text = soup.get_text(separator='\n', strip=True)

        # Clean up excessive whitespace
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        text = '\n'.join(lines)

        # Extract title if present
        title = None
        if soup.title and soup.title.string:
            title = soup.title.string.strip()

        return {
            'text': text,
            'title': title
        }

    except ImportError:
        print("\nError: beautifulsoup4 not installed. Run: pip install beautifulsoup4")
        return None
    except Exception as e:
        print(f"\nError extracting {filepath}: {e}")
        return None


def get_relative_path(file_path, source_folder):
    """Get the relative path of a file from the source folder."""
    try:
        return str(Path(file_path).relative_to(source_folder))
    except ValueError:
        return str(file_path)


def extract_all(source_folder, index_folder, reindex=False):
    """Extract text from all HTML files and save to index folder."""
    source_path = Path(source_folder)
    index_path = Path(index_folder)
    texts_path = index_path / 'texts'

    # Create folders
    texts_path.mkdir(parents=True, exist_ok=True)

    # Find all HTML files
    print(f"Scanning for HTML files in: {source_folder}")
    html_files = find_html_files(source_folder)
    print(f"Found {len(html_files)} HTML files")

    if not html_files:
        print("No HTML files found.")
        return

    # Track statistics
    processed = 0
    skipped = 0
    errors = 0

    # Load existing metadata if present
    metadata_file = index_path / 'metadata.json'
    metadata = {}
    if metadata_file.exists():
        with open(metadata_file, 'r', encoding='utf-8') as f:
            metadata = json.load(f)

    # Process each HTML file
    for html_path in tqdm(html_files, desc="Extracting text", unit="file"):
        filename = html_path.name
        # Use .html.json to avoid collision with other extractors
        output_file = texts_path / f"{filename}.json"

        # Skip if already extracted (unless reindex)
        if output_file.exists() and not reindex:
            skipped += 1
            continue

        # Extract text
        result = extract_text_from_html(html_path)

        if result is None:
            errors += 1
            continue

        # Save to JSON (same format as other extractors)
        doc = {
            'filename': filename,
            'path': get_relative_path(html_path, source_folder),
            'file_type': 'html',
            'pages': [
                {
                    'page_num': 1,
                    'text': result['text']
                }
            ]
        }

        # Add title if extracted
        if result.get('title'):
            doc['title'] = result['title']

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)

        processed += 1

    # Update metadata
    metadata['html_source_folder'] = str(source_folder)
    metadata['html_total_docs'] = processed + skipped
    metadata['html_extracted_at'] = datetime.now().isoformat()
    metadata['html_processed'] = processed
    metadata['html_skipped'] = skipped
    metadata['html_errors'] = errors

    # Update cumulative total_docs
    total_docs = metadata.get('total_docs', 0)
    if 'html_total_docs' not in metadata or reindex:
        metadata['total_docs'] = total_docs + processed

    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2)

    # Print summary
    print(f"\n--- HTML Extraction Complete ---")
    print(f"Processed: {processed} files")
    print(f"Skipped (already extracted): {skipped} files")
    print(f"Errors: {errors} files")
    print(f"Index saved to: {index_folder}")


def resolve_project_config(config, project_id):
    """Resolve source/index folders from a project entry in config.json."""
    projects = config.get('projects', [])
    for p in projects:
        if p['id'] == project_id:
            return p
    print(f"Error: Project '{project_id}' not found in config.json")
    print(f"Available projects: {[p['id'] for p in projects]}")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description='Extract text from HTML files for searching')
    parser.add_argument('--project', help='Project ID (reads config from config.json projects array)')
    parser.add_argument('--source', help='Source folder containing HTML files (overrides config.json)')
    parser.add_argument('--index', help='Index folder for extracted text (overrides config.json)')
    parser.add_argument('--reindex', action='store_true', help='Force re-extract all files')
    args = parser.parse_args()

    # Load config
    config = load_config()

    # Resolve settings from project config or flat config
    if args.project:
        project = resolve_project_config(config, args.project)
        source_folder = args.source or project.get('html_source_folder') or project.get('source_folder', '')
        index_folder = args.index or project.get('index_folder', f'./index/{args.project}')
    else:
        # Backward compat: flat config or --source/--index overrides
        source_folder = args.source or config.get('html_source_folder') or config.get('source_folder', '')
        index_folder = args.index or config.get('index_folder', './index')

    if not source_folder:
        print("Error: No source folder specified.")
        print("Set 'html_source_folder' in config.json or use --source/--project argument.")
        sys.exit(1)

    print(f"Source: {source_folder}")
    print(f"Index:  {index_folder}")
    print(f"Reindex: {args.reindex}")
    if args.project:
        print(f"Project: {args.project}")
    print()

    extract_all(source_folder, index_folder, reindex=args.reindex)


if __name__ == '__main__':
    main()
