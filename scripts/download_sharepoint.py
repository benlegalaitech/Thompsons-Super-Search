#!/usr/bin/env python3
"""Download files from SharePoint via Microsoft Graph API.

Requires Azure CLI to be logged in with permissions to access SharePoint.
Uses 'az rest' to call the Graph API.

Usage:
    python download_sharepoint.py
"""

import os
import sys
import json
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

# SharePoint site info
SITE_ID = "thompsonsscotland.sharepoint.com,4da7f75e-9861-4dbd-813e-6fc2682c4d9a,749e5499-7936-4ba8-ab48-7ab4da6b6fa1"
BASE_PATH = "We Robots"

# Folders to download (relative to BASE_PATH)
FOLDERS_TO_DOWNLOAD = [
    # Open Files (active cases)
    "Open Files/Adair(N22G0077)",
    "Open Files/Adams(A23G0425)",
    "Open Files/Allan(F23G0052)",
    # Closed Files
    "Closed Files/Closed Files from Graeme/05G - Destroy 2026",
    "Closed Files/Closed Files from Graeme/07G - Destroy 2028",
    # Academic Papers
    "Academic Papers/PRE-LITIGATION/Defenders",
    "Academic Papers/PRE-LITIGATION/Expert Witness",
    "Academic Papers/PRE-LITIGATION/Freedom of Information",
]

# Local destination
LOCAL_DEST = Path(r"C:\Users\benja\LungDiseaseSubset")


def az_rest(url: str) -> dict:
    """Call Microsoft Graph API via az rest."""
    result = subprocess.run(
        f'az rest --method GET --url "{url}"',
        capture_output=True,
        text=True,
        shell=True
    )
    if result.returncode != 0:
        return {}
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return {}


def get_folder_contents(folder_path: str) -> list:
    """Get all items in a SharePoint folder recursively."""
    items = []
    full_path = f"{BASE_PATH}/{folder_path}" if folder_path else BASE_PATH
    encoded_path = quote(full_path, safe='')

    url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drive/root:/{encoded_path}:/children?$top=999"

    while url:
        data = az_rest(url)
        if not data:
            break

        for item in data.get('value', []):
            if 'folder' in item:
                # Recursively get folder contents
                subfolder_path = f"{folder_path}/{item['name']}" if folder_path else item['name']
                items.extend(get_folder_contents(subfolder_path))
            else:
                # It's a file
                items.append({
                    'name': item['name'],
                    'path': folder_path,
                    'id': item['id'],
                    'size': item.get('size', 0),
                    'downloadUrl': item.get('@microsoft.graph.downloadUrl', '')
                })

        url = data.get('@odata.nextLink', '')

    return items


def download_file(file_info: dict, base_folder: str) -> tuple:
    """Download a single file."""
    import requests

    try:
        # Construct local path
        relative_path = file_info['path']
        if relative_path.startswith(base_folder):
            relative_path = relative_path[len(base_folder):].lstrip('/')

        local_dir = LOCAL_DEST / base_folder.replace('/', os.sep) / relative_path.replace('/', os.sep)
        local_dir.mkdir(parents=True, exist_ok=True)
        local_path = local_dir / file_info['name']

        # Skip if already exists with correct size
        if local_path.exists() and local_path.stat().st_size == file_info['size']:
            return ('skipped', file_info['name'], file_info['size'])

        # Get download URL
        download_url = file_info.get('downloadUrl')
        if not download_url:
            # Need to fetch the download URL
            full_path = f"{BASE_PATH}/{file_info['path']}/{file_info['name']}"
            encoded_path = quote(full_path, safe='')
            url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drive/root:/{encoded_path}"
            data = az_rest(url)
            download_url = data.get('@microsoft.graph.downloadUrl', '')

        if not download_url:
            return ('error', file_info['name'], 'No download URL')

        # Download using requests
        response = requests.get(download_url, stream=True, timeout=60)
        response.raise_for_status()

        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        return ('downloaded', file_info['name'], file_info['size'])
    except Exception as e:
        return ('error', file_info['name'], str(e))


def download_folder(folder_path: str, max_workers: int = 5):
    """Download all files from a SharePoint folder."""
    print(f"\n{'='*60}")
    print(f"Processing: {folder_path}")
    print(f"{'='*60}")

    # Get all files in folder
    print("Listing files...")
    files = get_folder_contents(folder_path)

    if not files:
        print(f"No files found in {folder_path}")
        return 0, 0, 0

    total_size = sum(f['size'] for f in files)
    print(f"Found {len(files)} files ({total_size / 1024 / 1024:.1f} MB)")

    downloaded = 0
    skipped = 0
    errors = 0
    downloaded_size = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_file, f, folder_path): f for f in files}

        for i, future in enumerate(as_completed(futures), 1):
            status, name, detail = future.result()

            if status == 'downloaded':
                downloaded += 1
                downloaded_size += detail
                if downloaded % 10 == 0 or downloaded == 1:
                    print(f"  [{i}/{len(files)}] Downloaded: {name}")
            elif status == 'skipped':
                skipped += 1
            else:
                errors += 1
                print(f"  ERROR: {name}: {detail}")

    print(f"\nFolder complete: {downloaded} downloaded, {skipped} skipped, {errors} errors")
    print(f"Downloaded size: {downloaded_size / 1024 / 1024:.1f} MB")

    return downloaded, skipped, errors


def main():
    print("SharePoint Download Script")
    print(f"Destination: {LOCAL_DEST}")
    print(f"Folders to download: {len(FOLDERS_TO_DOWNLOAD)}")

    # Create destination directory
    LOCAL_DEST.mkdir(parents=True, exist_ok=True)

    total_downloaded = 0
    total_skipped = 0
    total_errors = 0

    for folder in FOLDERS_TO_DOWNLOAD:
        d, s, e = download_folder(folder)
        total_downloaded += d
        total_skipped += s
        total_errors += e

    print(f"\n{'='*60}")
    print("DOWNLOAD COMPLETE")
    print(f"{'='*60}")
    print(f"Total downloaded: {total_downloaded}")
    print(f"Total skipped: {total_skipped}")
    print(f"Total errors: {total_errors}")


if __name__ == '__main__':
    main()
