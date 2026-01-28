#!/usr/bin/env python3
"""Copy all index blobs from root to a project prefix."""
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.storage.blob import BlobServiceClient

storage_account = os.environ.get('AZURE_STORAGE_ACCOUNT', 'thompsonstorage123')
storage_key = os.environ.get('AZURE_STORAGE_KEY')
project_id = 'ford'

account_url = f"https://{storage_account}.blob.core.windows.net"
blob_service_client = BlobServiceClient(account_url=account_url, credential=storage_key)
container_client = blob_service_client.get_container_client('index')

# Get all blobs without ford/ prefix
blobs_to_copy = []
for blob in container_client.list_blobs():
    if not blob.name.startswith('ford/') and not blob.name.startswith('lung-disease/'):
        blobs_to_copy.append(blob.name)

print(f"Found {len(blobs_to_copy)} blobs to copy to ford/ prefix")

def copy_blob(blob_name):
    try:
        source_url = f"{account_url}/index/{blob_name}"
        dest_name = f"{project_id}/{blob_name}"
        dest_blob = container_client.get_blob_client(dest_name)
        if dest_blob.exists():
            return ('skipped', blob_name)
        dest_blob.start_copy_from_url(source_url)
        return ('copied', blob_name)
    except Exception as e:
        return ('error', f"{blob_name}: {e}")

copied = 0
skipped = 0
errors = 0

with ThreadPoolExecutor(max_workers=50) as executor:
    futures = [executor.submit(copy_blob, name) for name in blobs_to_copy]
    for i, future in enumerate(as_completed(futures), 1):
        status, detail = future.result()
        if status == 'copied':
            copied += 1
        elif status == 'skipped':
            skipped += 1
        else:
            errors += 1
            print(f"Error: {detail}")
        if i % 1000 == 0:
            print(f"Progress: {i}/{len(blobs_to_copy)} ({copied} copied, {skipped} skipped)")

print(f"\nComplete! Copied: {copied}, Skipped: {skipped}, Errors: {errors}")
