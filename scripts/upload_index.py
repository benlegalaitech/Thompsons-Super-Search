#!/usr/bin/env python3
"""Upload the local index folder to Azure Blob Storage."""

import os
import sys
from pathlib import Path

try:
    from azure.storage.blob import BlobServiceClient
except ImportError:
    print("Error: azure-storage-blob not installed. Run: pip install azure-storage-blob")
    sys.exit(1)


def upload_index(storage_account: str, storage_key: str, index_folder: str, container: str = 'index', project_id: str = ''):
    """Upload all index files to blob storage."""
    account_url = f"https://{storage_account}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url=account_url, credential=storage_key)
    container_client = blob_service_client.get_container_client(container)

    # Ensure container exists
    try:
        container_client.create_container()
        print(f"Created container: {container}")
    except Exception:
        pass  # Container already exists

    index_path = Path(index_folder)
    if not index_path.exists():
        print(f"Error: Index folder not found: {index_folder}")
        sys.exit(1)

    uploaded = 0
    total_size = 0

    for file_path in index_path.rglob('*'):
        if file_path.is_file():
            relative = str(file_path.relative_to(index_path)).replace('\\', '/')
            # Prefix with project_id if specified
            blob_name = f"{project_id}/{relative}" if project_id else relative
            file_size = file_path.stat().st_size

            print(f"Uploading: {blob_name} ({file_size:,} bytes)")

            with open(file_path, 'rb') as f:
                container_client.upload_blob(name=blob_name, data=f, overwrite=True)

            uploaded += 1
            total_size += file_size

    print(f"\nUpload complete!")
    print(f"  Files: {uploaded}")
    print(f"  Total size: {total_size:,} bytes ({total_size / 1024 / 1024:.2f} MB)")
    print(f"  Container: {container}")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Upload index to Azure Blob Storage')
    parser.add_argument('index_folder', nargs='?', default='./index', help='Index folder to upload')
    parser.add_argument('--project', help='Project ID (prefixes blob names with project_id/)')
    args = parser.parse_args()

    storage_account = os.environ.get('AZURE_STORAGE_ACCOUNT', 'thompsonstorage123')
    storage_key = os.environ.get('AZURE_STORAGE_KEY')

    if not storage_key:
        print("Error: AZURE_STORAGE_KEY environment variable required")
        print("Set it with: set AZURE_STORAGE_KEY=<your-key>")
        sys.exit(1)

    print(f"Uploading index from: {args.index_folder}")
    print(f"To storage account: {storage_account}")
    if args.project:
        print(f"Project prefix: {args.project}/")
    print()

    upload_index(storage_account, storage_key, args.index_folder, project_id=args.project or '')
