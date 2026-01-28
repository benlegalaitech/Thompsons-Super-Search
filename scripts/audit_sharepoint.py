#!/usr/bin/env python3
"""Audit files in SharePoint to understand file types and structure.

Usage:
    python audit_sharepoint.py
"""

import subprocess
import json
from collections import defaultdict
from urllib.parse import quote

SITE_ID = "thompsonsscotland.sharepoint.com,4da7f75e-9861-4dbd-813e-6fc2682c4d9a,749e5499-7936-4ba8-ab48-7ab4da6b6fa1"
BASE_PATH = "We Robots"


def az_rest(url: str) -> dict:
    """Call Graph API."""
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


def audit_folder(folder_path: str, depth: int = 0, max_files: int = 500) -> dict:
    """Audit files in a folder, returning file type statistics."""
    stats = {
        'extensions': defaultdict(lambda: {'count': 0, 'size': 0}),
        'total_files': 0,
        'total_size': 0,
        'folders': 0,
        'sample_files': []
    }

    full_path = f"{BASE_PATH}/{folder_path}" if folder_path else BASE_PATH
    encoded_path = quote(full_path, safe='')
    url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drive/root:/{encoded_path}:/children?$top=999"

    while url and stats['total_files'] < max_files:
        data = az_rest(url)
        if not data:
            break

        for item in data.get('value', []):
            if 'folder' in item:
                stats['folders'] += 1
                # Recurse into subfolders (but limit depth)
                if depth < 2:
                    subfolder_path = f"{folder_path}/{item['name']}" if folder_path else item['name']
                    sub_stats = audit_folder(subfolder_path, depth + 1, max_files - stats['total_files'])
                    # Merge stats
                    for ext, data in sub_stats['extensions'].items():
                        stats['extensions'][ext]['count'] += data['count']
                        stats['extensions'][ext]['size'] += data['size']
                    stats['total_files'] += sub_stats['total_files']
                    stats['total_size'] += sub_stats['total_size']
                    stats['folders'] += sub_stats['folders']
            else:
                # It's a file
                name = item['name']
                size = item.get('size', 0)
                ext = name.rsplit('.', 1)[-1].lower() if '.' in name else 'no_extension'

                stats['extensions'][ext]['count'] += 1
                stats['extensions'][ext]['size'] += size
                stats['total_files'] += 1
                stats['total_size'] += size

                # Keep some sample filenames
                if len(stats['sample_files']) < 20:
                    stats['sample_files'].append(name)

        url = data.get('@odata.nextLink', '')

    return stats


def format_size(size_bytes):
    """Format bytes as human readable."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def main():
    folders_to_audit = [
        "Open Files",
        "Closed Files",
        "Academic Papers",
        "Additional Test Files",
    ]

    print("SharePoint File Audit")
    print("=" * 70)

    all_extensions = defaultdict(lambda: {'count': 0, 'size': 0})

    for folder in folders_to_audit:
        print(f"\nAuditing: {folder}")
        print("-" * 50)

        stats = audit_folder(folder, max_files=1000)

        print(f"Files scanned: {stats['total_files']}")
        print(f"Total size: {format_size(stats['total_size'])}")
        print(f"Subfolders: {stats['folders']}")

        print("\nFile types found:")
        sorted_exts = sorted(stats['extensions'].items(), key=lambda x: -x[1]['count'])
        for ext, data in sorted_exts[:15]:
            print(f"  .{ext:12} {data['count']:5} files  ({format_size(data['size'])})")

        # Aggregate
        for ext, data in stats['extensions'].items():
            all_extensions[ext]['count'] += data['count']
            all_extensions[ext]['size'] += data['size']

        if stats['sample_files']:
            print("\nSample files:")
            for f in stats['sample_files'][:5]:
                print(f"  - {f}")

    print("\n" + "=" * 70)
    print("OVERALL FILE TYPE SUMMARY")
    print("=" * 70)

    sorted_all = sorted(all_extensions.items(), key=lambda x: -x[1]['count'])
    for ext, data in sorted_all:
        print(f"  .{ext:12} {data['count']:5} files  ({format_size(data['size'])})")


if __name__ == '__main__':
    main()
