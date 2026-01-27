#!/usr/bin/env python3
"""Upload Excel source files from NATIVES folder to Azure Blob Storage.

Uploads to the same 'pdfs' container used for PDF source files, so the
web app's blob-based file serving works for both types.
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

EXCEL_EXTENSIONS = {'.xls', '.xlsx', '.xlsm', '.xlsb'}


def upload_excel_files(storage_account: str, storage_key: str, source_folder: str,
                       container: str = 'pdfs', max_workers: int = 10):
    """Upload all Excel files from source_folder to blob storage."""
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

    # Collect all Excel files
    files_to_upload = []
    for file_path in source_path.rglob('*'):
        if file_path.is_file() and file_path.suffix.lower() in EXCEL_EXTENSIONS:
            blob_name = str(file_path.relative_to(source_path)).replace('\\', '/')
            files_to_upload.append((file_path, blob_name))

    if not files_to_upload:
        print("No Excel files found to upload.")
        return

    print(f"Found {len(files_to_upload)} Excel files to upload")
    print(f"Container: {container}")
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
                    print(f"  [{i}/{len(files_to_upload)}] Uploaded: {blob_name} ({detail:,} bytes)")
            elif status == 'skipped':
                skipped += 1
            else:
                errors += 1
                print(f"  ERROR: {blob_name}: {detail}")

            # Progress update every 200 files
            if i % 200 == 0:
                print(f"  Progress: {i}/{len(files_to_upload)} processed ({uploaded} uploaded, {skipped} skipped, {errors} errors)")

    print(f"\nUpload complete!")
    print(f"  Uploaded: {uploaded}")
    print(f"  Skipped (already exists): {skipped}")
    print(f"  Errors: {errors}")
    print(f"  Total uploaded size: {total_size:,} bytes ({total_size / 1024 / 1024:.1f} MB)")


if __name__ == '__main__':
    storage_account = os.environ.get('AZURE_STORAGE_ACCOUNT', 'thompsonstorage123')
    storage_key = os.environ.get('AZURE_STORAGE_KEY')

    if not storage_key:
        print("Error: AZURE_STORAGE_KEY environment variable required")
        print("Set it with: set AZURE_STORAGE_KEY=<your-key>")
        sys.exit(1)

    # Get source folder from argument or config.json
    if len(sys.argv) > 1:
        source_folder = sys.argv[1]
    else:
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config.json')
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            source_folder = config.get('excel_source_folder', '')
        else:
            source_folder = ''

    if not source_folder:
        print("Error: No source folder specified.")
        print("Usage: python upload_excel_files.py [source_folder]")
        print("Or set 'excel_source_folder' in config.json")
        sys.exit(1)

    print(f"Uploading Excel files from: {source_folder}")
    print(f"To storage account: {storage_account}")
    print()

    upload_excel_files(storage_account, storage_key, source_folder)
