#!/usr/bin/env python3
"""
Outlook Email Text Extraction Script

Extracts text from Outlook email files (.msg) and saves as JSON for searching.
Extracts subject, sender, recipients, date, and body text.

Usage:
    python extract_email.py --project lung-disease
    python extract_email.py --source /path
    python extract_email.py --reindex
"""

import os
import sys
import json
import argparse
from datetime import datetime
from pathlib import Path

from tqdm import tqdm

EMAIL_EXTENSIONS = ['.msg']


def load_config():
    """Load configuration from config.json."""
    config_path = Path('config.json')
    if not config_path.exists():
        print("Error: config.json not found.")
        sys.exit(1)

    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def find_email_files(source_folder, extensions=None):
    """Find all email files in the source folder recursively."""
    if extensions is None:
        extensions = EMAIL_EXTENSIONS

    files = []
    source_path = Path(source_folder)

    if not source_path.exists():
        print(f"Error: Source folder does not exist: {source_folder}")
        sys.exit(1)

    for ext in extensions:
        files.extend(source_path.rglob(f'*{ext}'))
        files.extend(source_path.rglob(f'*{ext.upper()}'))

    return sorted(set(files))


def extract_text_from_msg(filepath):
    """Extract text from .msg file using extract-msg library."""
    try:
        import extract_msg

        msg = extract_msg.Message(str(filepath))

        # Build searchable text from email components
        parts = []

        if msg.subject:
            parts.append(f"Subject: {msg.subject}")

        if msg.sender:
            parts.append(f"From: {msg.sender}")

        if msg.to:
            parts.append(f"To: {msg.to}")

        if msg.cc:
            parts.append(f"CC: {msg.cc}")

        if msg.date:
            parts.append(f"Date: {msg.date}")

        parts.append("")  # Blank line before body

        if msg.body:
            parts.append(msg.body)

        # Also include attachment names for searchability
        if msg.attachments:
            attach_names = [a.longFilename or a.shortFilename for a in msg.attachments if a.longFilename or a.shortFilename]
            if attach_names:
                parts.append(f"\nAttachments: {', '.join(attach_names)}")

        msg.close()

        return {
            'text': '\n'.join(parts),
            'subject': msg.subject or '',
            'sender': str(msg.sender) if msg.sender else '',
            'date': str(msg.date) if msg.date else ''
        }

    except Exception as e:
        print(f"\nError extracting {filepath}: {e}")
        return None


def get_relative_path(file_path, source_folder):
    """Get the relative path of a file from the source folder."""
    try:
        return str(Path(file_path).relative_to(source_folder))
    except ValueError:
        return str(file_path)


def extract_all(source_folder, index_folder, reindex=False, extensions=None):
    """Extract text from all email files and save to index folder."""
    source_path = Path(source_folder)
    index_path = Path(index_folder)
    texts_path = index_path / 'texts'

    texts_path.mkdir(parents=True, exist_ok=True)

    print(f"Scanning for email files in: {source_folder}")
    files = find_email_files(source_folder, extensions)
    print(f"Found {len(files)} email files")

    if not files:
        print("No email files found.")
        return

    total_docs = 0
    processed = 0
    skipped = 0
    errors = 0

    for filepath in tqdm(files, desc="Extracting text", unit="file"):
        filename = filepath.name
        output_file = texts_path / f"{filepath.name}.json"

        if output_file.exists() and not reindex:
            skipped += 1
            continue

        result = extract_text_from_msg(filepath)

        if result is None:
            errors += 1
            continue

        doc = {
            'filename': filename,
            'path': get_relative_path(filepath, source_folder),
            'file_type': 'email',
            'pages': [{
                'page_num': 1,
                'text': result['text']
            }]
        }

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)

        total_docs += 1
        processed += 1

    # Update metadata
    metadata_file = index_path / 'metadata.json'
    metadata = {}
    if metadata_file.exists():
        with open(metadata_file, 'r', encoding='utf-8') as f:
            metadata = json.load(f)

    metadata['email_source_folder'] = str(source_folder)
    metadata['email_total_docs'] = processed + skipped
    metadata['email_extracted_at'] = datetime.now().isoformat()
    metadata['email_processed'] = processed
    metadata['email_skipped'] = skipped
    metadata['email_errors'] = errors

    # Update totals
    metadata['total_docs'] = metadata.get('total_docs', 0) + processed
    if 'email_docs' not in metadata:
        metadata['email_docs'] = processed + skipped

    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2)

    print(f"\n--- Email Extraction Complete ---")
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
    parser = argparse.ArgumentParser(description='Extract text from Outlook emails for searching')
    parser.add_argument('--project', help='Project ID (reads config from config.json projects array)')
    parser.add_argument('--source', help='Source folder containing email files (overrides config.json)')
    parser.add_argument('--index', help='Index folder for extracted text (overrides config.json)')
    parser.add_argument('--reindex', action='store_true', help='Force re-extract all files')
    args = parser.parse_args()

    config = load_config()

    if args.project:
        project = resolve_project_config(config, args.project)
        source_folder = args.source or project.get('source_folder', '')
        index_folder = args.index or project.get('index_folder', f'./index/{args.project}')
        extensions = project.get('email_extensions', EMAIL_EXTENSIONS)
    else:
        source_folder = args.source or config.get('source_folder', '')
        index_folder = args.index or config.get('index_folder', './index')
        extensions = config.get('email_extensions', EMAIL_EXTENSIONS)

    if not source_folder:
        print("Error: No source folder specified.")
        print("Set 'source_folder' in config.json or use --source/--project argument.")
        sys.exit(1)

    print(f"Source: {source_folder}")
    print(f"Index:  {index_folder}")
    print(f"Extensions: {extensions}")
    print(f"Reindex: {args.reindex}")
    if args.project:
        print(f"Project: {args.project}")
    print()

    extract_all(source_folder, index_folder, reindex=args.reindex, extensions=extensions)


if __name__ == '__main__':
    main()
