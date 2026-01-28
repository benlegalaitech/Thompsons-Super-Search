#!/usr/bin/env python3
"""
Word Document Text Extraction Script

Extracts text from Word documents (.doc, .docx) and saves as JSON for searching.
Produces the same index format as extract.py (for PDFs).

Usage:
    python extract_word.py --project lung-disease
    python extract_word.py --source /path
    python extract_word.py --reindex
"""

import os
import sys
import json
import argparse
from datetime import datetime
from pathlib import Path

from tqdm import tqdm

WORD_EXTENSIONS = ['.doc', '.docx']


def load_config():
    """Load configuration from config.json."""
    config_path = Path('config.json')
    if not config_path.exists():
        print("Error: config.json not found.")
        sys.exit(1)

    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def find_word_files(source_folder, extensions=None):
    """Find all Word files in the source folder recursively."""
    if extensions is None:
        extensions = WORD_EXTENSIONS

    files = []
    source_path = Path(source_folder)

    if not source_path.exists():
        print(f"Error: Source folder does not exist: {source_folder}")
        sys.exit(1)

    for ext in extensions:
        files.extend(source_path.rglob(f'*{ext}'))
        files.extend(source_path.rglob(f'*{ext.upper()}'))

    return sorted(set(files))


def extract_text_from_docx(filepath):
    """Extract text from .docx file using python-docx."""
    try:
        from docx import Document
        doc = Document(str(filepath))
        paragraphs = []
        for para in doc.paragraphs:
            if para.text.strip():
                paragraphs.append(para.text.strip())
        return '\n\n'.join(paragraphs)
    except Exception as e:
        print(f"\nError extracting {filepath}: {e}")
        return None


def extract_text_from_doc(filepath):
    """Extract text from .doc file (Word 97-2003).

    Tries multiple methods:
    1. antiword (if installed)
    2. textract (if installed)
    3. win32com (Windows only)
    """
    import subprocess

    # Try antiword first (cross-platform)
    try:
        result = subprocess.run(
            ['antiword', str(filepath)],
            capture_output=True,
            text=True,
            timeout=30
        )
        if result.returncode == 0:
            return result.stdout
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    # Try using python-docx2txt
    try:
        import docx2txt
        text = docx2txt.process(str(filepath))
        if text and text.strip():
            return text
    except Exception:
        pass

    # Try win32com on Windows
    if sys.platform == 'win32':
        try:
            import win32com.client
            word = win32com.client.Dispatch("Word.Application")
            word.Visible = False
            doc = word.Documents.Open(str(filepath.absolute()))
            text = doc.Content.Text
            doc.Close(False)
            word.Quit()
            return text
        except Exception as e:
            pass

    # Fallback: try to read raw text (works for some .doc files)
    try:
        with open(filepath, 'rb') as f:
            content = f.read()
            # Try to extract ASCII text from binary
            text_parts = []
            current = []
            for byte in content:
                if 32 <= byte <= 126 or byte in (9, 10, 13):
                    current.append(chr(byte))
                else:
                    if len(current) > 20:  # Only keep meaningful strings
                        text_parts.append(''.join(current))
                    current = []
            if current and len(current) > 20:
                text_parts.append(''.join(current))
            if text_parts:
                return '\n'.join(text_parts)
    except Exception:
        pass

    return None


def extract_text_from_word(filepath):
    """Extract text from a Word document.

    Returns dict with 'text' or None on error.
    """
    ext = filepath.suffix.lower()

    if ext == '.docx':
        text = extract_text_from_docx(filepath)
    elif ext == '.doc':
        text = extract_text_from_doc(filepath)
    else:
        return None

    if text:
        return {'text': text.strip()}
    return None


def get_relative_path(file_path, source_folder):
    """Get the relative path of a file from the source folder."""
    try:
        return str(Path(file_path).relative_to(source_folder))
    except ValueError:
        return str(file_path)


def extract_all(source_folder, index_folder, reindex=False, extensions=None):
    """Extract text from all Word files and save to index folder."""
    source_path = Path(source_folder)
    index_path = Path(index_folder)
    texts_path = index_path / 'texts'

    texts_path.mkdir(parents=True, exist_ok=True)

    print(f"Scanning for Word files in: {source_folder}")
    files = find_word_files(source_folder, extensions)
    print(f"Found {len(files)} Word files")

    if not files:
        print("No Word files found.")
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

        result = extract_text_from_word(filepath)

        if result is None:
            errors += 1
            continue

        doc = {
            'filename': filename,
            'path': get_relative_path(filepath, source_folder),
            'file_type': 'word',
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

    metadata['word_source_folder'] = str(source_folder)
    metadata['word_total_docs'] = processed + skipped
    metadata['word_extracted_at'] = datetime.now().isoformat()
    metadata['word_processed'] = processed
    metadata['word_skipped'] = skipped
    metadata['word_errors'] = errors

    # Update totals
    metadata['total_docs'] = metadata.get('total_docs', 0) + processed
    if 'word_docs' not in metadata:
        metadata['word_docs'] = processed + skipped

    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2)

    print(f"\n--- Word Extraction Complete ---")
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
    parser = argparse.ArgumentParser(description='Extract text from Word documents for searching')
    parser.add_argument('--project', help='Project ID (reads config from config.json projects array)')
    parser.add_argument('--source', help='Source folder containing Word files (overrides config.json)')
    parser.add_argument('--index', help='Index folder for extracted text (overrides config.json)')
    parser.add_argument('--reindex', action='store_true', help='Force re-extract all files')
    args = parser.parse_args()

    config = load_config()

    if args.project:
        project = resolve_project_config(config, args.project)
        source_folder = args.source or project.get('source_folder', '')
        index_folder = args.index or project.get('index_folder', f'./index/{args.project}')
        extensions = project.get('word_extensions', WORD_EXTENSIONS)
    else:
        source_folder = args.source or config.get('source_folder', '')
        index_folder = args.index or config.get('index_folder', './index')
        extensions = config.get('word_extensions', WORD_EXTENSIONS)

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
