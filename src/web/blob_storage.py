"""Azure Blob Storage integration for PDF serving and index loading."""

import os
import sys
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions


# Configuration from environment
STORAGE_ACCOUNT_NAME = os.environ.get('AZURE_STORAGE_ACCOUNT', '')
STORAGE_ACCOUNT_KEY = os.environ.get('AZURE_STORAGE_KEY', '')
PDF_CONTAINER = os.environ.get('AZURE_PDF_CONTAINER', 'pdfs')
INDEX_CONTAINER = os.environ.get('AZURE_INDEX_CONTAINER', 'index')

# Per-project download state: {project_id: {'in_progress': bool, 'complete': bool, 'thread': Thread}}
_download_states = {}
_download_lock = threading.Lock()


def is_blob_storage_enabled():
    """Check if blob storage is configured."""
    return bool(STORAGE_ACCOUNT_NAME and STORAGE_ACCOUNT_KEY)


def get_blob_service_client():
    """Get blob service client."""
    if not is_blob_storage_enabled():
        raise RuntimeError("Azure Blob Storage not configured")

    account_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
    return BlobServiceClient(account_url=account_url, credential=STORAGE_ACCOUNT_KEY)


def generate_pdf_sas_url(blob_path: str, expiry_hours: int = 1) -> str:
    """Generate a SAS URL for a PDF blob with time-limited access."""
    if not is_blob_storage_enabled():
        raise RuntimeError("Azure Blob Storage not configured")

    sas_token = generate_blob_sas(
        account_name=STORAGE_ACCOUNT_NAME,
        container_name=PDF_CONTAINER,
        blob_name=blob_path,
        account_key=STORAGE_ACCOUNT_KEY,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=expiry_hours)
    )
    return f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{PDF_CONTAINER}/{blob_path}?{sas_token}"


def check_blob_exists(blob_path: str) -> bool:
    """Check if a PDF blob exists in storage."""
    if not is_blob_storage_enabled():
        return False

    blob_service_client = get_blob_service_client()
    blob_client = blob_service_client.get_blob_client(PDF_CONTAINER, blob_path)
    return blob_client.exists()


def _download_single_blob(args):
    """Download a single blob (for parallel execution)."""
    container_client, blob_name, local_index_folder, strip_prefix = args
    try:
        blob_client = container_client.get_blob_client(blob_name)
        # Strip the project prefix from blob name for local path
        local_name = blob_name
        if strip_prefix and blob_name.startswith(strip_prefix):
            local_name = blob_name[len(strip_prefix):]
        local_path = os.path.join(local_index_folder, local_name)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, 'wb') as f:
            f.write(blob_client.download_blob().readall())
        return True
    except Exception as e:
        print(f"Error downloading {blob_name}: {e}")
        return False


def is_index_download_complete(project_id='default'):
    """Check if index download has completed for a project."""
    state = _download_states.get(project_id, {})
    return state.get('complete', False)


def is_index_downloading(project_id='default'):
    """Check if index download is in progress for a project."""
    state = _download_states.get(project_id, {})
    return state.get('in_progress', False)


def download_index_from_blob(local_index_folder: str, project_id: str = 'default', blob_prefix: str = '') -> bool:
    """Download index files from blob storage to local folder (parallel).

    If blob_prefix is given, only downloads blobs matching that prefix
    and strips the prefix from local file paths.
    """
    with _download_lock:
        _download_states[project_id] = {'in_progress': True, 'complete': False, 'thread': None}

    if not is_blob_storage_enabled():
        print("Blob storage not configured, skipping index download", file=sys.stderr, flush=True)
        _download_states[project_id] = {'in_progress': False, 'complete': True, 'thread': None}
        return False

    print(f"Downloading index for project '{project_id}' from blob storage to {local_index_folder}...", file=sys.stderr, flush=True)

    os.makedirs(local_index_folder, exist_ok=True)
    texts_folder = os.path.join(local_index_folder, 'texts')
    os.makedirs(texts_folder, exist_ok=True)

    blob_service_client = get_blob_service_client()
    container_client = blob_service_client.get_container_client(INDEX_CONTAINER)

    try:
        # Filter by prefix if specified
        if blob_prefix:
            blob_list = list(container_client.list_blobs(name_starts_with=blob_prefix))
            strip_prefix = blob_prefix if blob_prefix.endswith('/') else blob_prefix + '/'
        else:
            blob_list = list(container_client.list_blobs())
            strip_prefix = ''

        if not blob_list:
            print(f"No index files found in blob storage for project '{project_id}'", file=sys.stderr, flush=True)
            _download_states[project_id] = {'in_progress': False, 'complete': True, 'thread': None}
            return False

        print(f"Found {len(blob_list)} index files for project '{project_id}', downloading in parallel...", file=sys.stderr, flush=True)

        # Download in parallel with 20 workers
        download_args = [(container_client, blob.name, local_index_folder, strip_prefix) for blob in blob_list]
        downloaded = 0

        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(_download_single_blob, args) for args in download_args]
            for future in as_completed(futures):
                if future.result():
                    downloaded += 1

        print(f"Downloaded {downloaded} index files for project '{project_id}' from blob storage", file=sys.stderr, flush=True)
        _download_states[project_id] = {'in_progress': False, 'complete': True, 'thread': None}
        return True
    except Exception as e:
        print(f"Error downloading index for project '{project_id}' from blob: {e}", file=sys.stderr, flush=True)
        _download_states[project_id] = {'in_progress': False, 'complete': True, 'thread': None}
        return False


def start_background_index_download(local_index_folder: str, project_id: str = 'default'):
    """Start downloading index in background thread for a specific project."""
    metadata_file = os.path.join(local_index_folder, 'metadata.json')
    if os.path.exists(metadata_file):
        print(f"Index for project '{project_id}' already exists locally, skipping download", file=sys.stderr, flush=True)
        _download_states[project_id] = {'in_progress': False, 'complete': True, 'thread': None}
        return

    state = _download_states.get(project_id, {})
    existing_thread = state.get('thread')
    if existing_thread is not None and existing_thread.is_alive():
        print(f"Index download for project '{project_id}' already in progress", file=sys.stderr, flush=True)
        return

    print(f"Starting background index download for project '{project_id}'...", file=sys.stderr, flush=True)
    # Use project_id as blob prefix so blobs are organized as {project_id}/texts/...
    thread = threading.Thread(
        target=download_index_from_blob,
        args=(local_index_folder,),
        kwargs={'project_id': project_id, 'blob_prefix': project_id},
        daemon=True
    )
    _download_states[project_id] = {'in_progress': True, 'complete': False, 'thread': thread}
    thread.start()
