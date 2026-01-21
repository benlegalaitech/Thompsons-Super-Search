#!/usr/bin/env python3
"""
PDF Text Extraction Script

Extracts text from PDFs page-by-page and saves as JSON for fast searching.

Usage:
    python extract.py                    # Run extraction using config.json
    python extract.py --source /path     # Override source folder
    python extract.py --reindex          # Force re-extract all files
"""

import os
import sys
import json
import argparse
from datetime import datetime
from pathlib import Path

import pdfplumber
from tqdm import tqdm


def load_config():
    """Load configuration from config.json."""
    config_path = Path('config.json')
    if not config_path.exists():
        print("Error: config.json not found.")
        print("Create config.json from config.example.json and set your PDF source folder.")
        sys.exit(1)

    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def find_pdfs(source_folder, extensions=None):
    """Find all PDF files in the source folder recursively."""
    if extensions is None:
        extensions = ['.pdf']

    pdfs = []
    source_path = Path(source_folder)

    if not source_path.exists():
        print(f"Error: Source folder does not exist: {source_folder}")
        sys.exit(1)

    for ext in extensions:
        pdfs.extend(source_path.rglob(f'*{ext}'))

    return sorted(pdfs)


def extract_text_from_pdf(pdf_path):
    """Extract text from all pages of a PDF."""
    pages = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ''
                pages.append({
                    'page_num': i,
                    'text': text.strip()
                })
    except Exception as e:
        print(f"\nError extracting {pdf_path}: {e}")
        return None

    return pages


def get_relative_path(pdf_path, source_folder):
    """Get the relative path of a PDF from the source folder."""
    try:
        return str(Path(pdf_path).relative_to(source_folder))
    except ValueError:
        return str(pdf_path)


def extract_all(source_folder, index_folder, reindex=False, extensions=None):
    """Extract text from all PDFs and save to index folder."""
    source_path = Path(source_folder)
    index_path = Path(index_folder)
    texts_path = index_path / 'texts'

    # Create folders
    texts_path.mkdir(parents=True, exist_ok=True)

    # Find all PDFs
    print(f"Scanning for PDFs in: {source_folder}")
    pdfs = find_pdfs(source_folder, extensions)
    print(f"Found {len(pdfs)} PDF files")

    if not pdfs:
        print("No PDF files found.")
        return

    # Track statistics
    total_pages = 0
    processed = 0
    skipped = 0
    errors = 0

    # Process each PDF
    for pdf_path in tqdm(pdfs, desc="Extracting text", unit="file"):
        filename = pdf_path.name
        output_file = texts_path / f"{pdf_path.stem}.json"

        # Skip if already extracted (unless reindex)
        if output_file.exists() and not reindex:
            skipped += 1
            continue

        # Extract text
        pages = extract_text_from_pdf(pdf_path)

        if pages is None:
            errors += 1
            continue

        # Save to JSON
        doc = {
            'filename': filename,
            'path': get_relative_path(pdf_path, source_folder),
            'pages': pages
        }

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)

        total_pages += len(pages)
        processed += 1

    # Save metadata
    metadata = {
        'source_folder': str(source_folder),
        'total_docs': processed + skipped,
        'total_pages': total_pages,
        'extracted_at': datetime.now().isoformat(),
        'processed': processed,
        'skipped': skipped,
        'errors': errors
    }

    with open(index_path / 'metadata.json', 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2)

    # Print summary
    print(f"\n--- Extraction Complete ---")
    print(f"Processed: {processed} files")
    print(f"Skipped (already extracted): {skipped} files")
    print(f"Errors: {errors} files")
    print(f"Total pages extracted: {total_pages}")
    print(f"Index saved to: {index_folder}")


def main():
    parser = argparse.ArgumentParser(description='Extract text from PDFs for searching')
    parser.add_argument('--source', help='Source folder containing PDFs (overrides config.json)')
    parser.add_argument('--index', help='Index folder for extracted text (overrides config.json)')
    parser.add_argument('--reindex', action='store_true', help='Force re-extract all files')
    args = parser.parse_args()

    # Load config
    config = load_config()

    # Get settings (command line args override config)
    source_folder = args.source or config.get('source_folder')
    index_folder = args.index or config.get('index_folder', './index')
    extensions = config.get('file_extensions', ['.pdf'])

    if not source_folder:
        print("Error: No source folder specified.")
        print("Set 'source_folder' in config.json or use --source argument.")
        sys.exit(1)

    print(f"Source: {source_folder}")
    print(f"Index:  {index_folder}")
    print(f"Reindex: {args.reindex}")
    print()

    extract_all(source_folder, index_folder, reindex=args.reindex, extensions=extensions)


if __name__ == '__main__':
    main()
