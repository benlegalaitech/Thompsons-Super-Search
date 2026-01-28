#!/usr/bin/env python3
"""Upload all source files (PDFs, Word, Excel, emails) to Azure Blob Storage.

Uploads to the 'pdfs' container so the web app's blob-based file serving works.
"""

import os
import sys
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from azure.storage.blob import BlobServiceClient
except ImportError:
    print("Error: azure-storage-blob not installed. Run: pip install azure-storage-blob")
    sys.exit(1)

# All supported file extensions
ALL_EXTENSIONS = {
    '.pdf',
    '.xls', '.xlsx', '.xlsm', '.xlsb',
    '.doc', '.docx',
    '.msg'
}


def upload_source_files(storage_account: str, storage_key: str, source_folder: str,
                        container: str = 'pdfs', max_workers: int = 10, project_prefix: str = ''):
    """Upload all source files from source_folder to blob storage."""
    account_url = f"https://{storage_account}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url=account_url, credential=storage_key)
    container_client = blob_service_client.get_container_client(container)

    # Ensure container exists
    try:
        container_client.create_container()
        print(f"Created container: {container}")
    except Exception:
        pass  # Container already exists

    source_path = Path(source_folder)
    if not source_path.exists():
        print(f"Error: Source folder not found: {source_folder}")
        sys.exit(1)

    # Collect all supported files
    files_to_upload = []
    for file_path in source_path.rglob('*'):
        if file_path.is_file() and file_path.suffix.lower() in ALL_EXTENSIONS:
            relative_path = str(file_path.relative_to(source_path)).replace('\\', '/')
            # Add project prefix if specified
            blob_name = f"{project_prefix}/{relative_path}" if project_prefix else relative_path
            files_to_upload.append((file_path, blob_name))

    if not files_to_upload:
        print("No supported files found to upload.")
        return

    # Count by type
    by_type = {}
    for fp, _ in files_to_upload:
        ext = fp.suffix.lower()
        by_type[ext] = by_type.get(ext, 0) + 1

    print(f"Found {len(files_to_upload)} files to upload:")
    for ext, count in sorted(by_type.items()):
        print(f"  {ext}: {count}")
    print(f"Container: {container}")
    if project_prefix:
        print(f"Project prefix: {project_prefix}/")
    print()

    uploaded = 0
    skipped = 0
    errors = 0
    total_size = 0

    def upload_one(item):
        file_path, blob_name = item
        try:
            blob_client = container_client.get_blob_client(blob_name)

            # Skip if already exists
            if blob_client.exists():
                return ('skipped', blob_name, 0)

            file_size = file_path.stat().st_size
            with open(file_path, 'rb') as f:
                blob_client.upload_blob(f, overwrite=False)
            return ('uploaded', blob_name, file_size)
        except Exception as e:
            return ('error', blob_name, str(e))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(upload_one, item): item for item in files_to_upload}
        for i, future in enumerate(as_completed(futures), 1):
            status, blob_name, detail = future.result()
            if status == 'uploaded':
                uploaded += 1
                total_size += detail
                if uploaded % 50 == 0 or uploaded == 1:
                    print(f"  [{i}/{len(files_to_upload)}] Uploaded: {blob_name}")
            elif status == 'skipped':
                skipped += 1
            else:
                errors += 1
                print(f"  ERROR: {blob_name}: {detail}")

            # Progress update every 100 files
            if i % 100 == 0:
                print(f"  Progress: {i}/{len(files_to_upload)} ({uploaded} uploaded, {skipped} skipped)")

    print(f"\nUpload complete!")
    print(f"  Uploaded: {uploaded}")
    print(f"  Skipped (already exists): {skipped}")
    print(f"  Errors: {errors}")
    print(f"  Total uploaded size: {total_size:,} bytes ({total_size / 1024 / 1024:.1f} MB)")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Upload source files to Azure Blob Storage')
    parser.add_argument('source_folder', nargs='?', default='', help='Source folder containing files')
    parser.add_argument('--project', help='Project ID (reads config and uses as blob prefix)')
    args = parser.parse_args()

    storage_account = os.environ.get('AZURE_STORAGE_ACCOUNT', 'thompsonstorage123')
    storage_key = os.environ.get('AZURE_STORAGE_KEY')

    if not storage_key:
        print("Error: AZURE_STORAGE_KEY environment variable required")
        print("Set it with: set AZURE_STORAGE_KEY=<your-key>")
        sys.exit(1)

    # Resolve source folder from config if --project specified
    source_folder = args.source_folder
    project_prefix = ''

    if args.project:
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config.json')
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            project = next((p for p in config.get('projects', []) if p['id'] == args.project), None)
            if project:
                if not source_folder:
                    source_folder = project.get('source_folder', '')
                project_prefix = args.project
            else:
                print(f"Error: Project '{args.project}' not found in config.json")
                sys.exit(1)

    if not source_folder:
        print("Error: No source folder specified.")
        print("Usage: python upload_source_files.py [source_folder] [--project project_id]")
        sys.exit(1)

    print(f"Uploading source files from: {source_folder}")
    print(f"To storage account: {storage_account}")
    if args.project:
        print(f"Project: {args.project}")
    print()

    upload_source_files(storage_account, storage_key, source_folder, project_prefix=project_prefix)
