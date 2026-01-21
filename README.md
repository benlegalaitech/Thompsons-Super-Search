# Thompsons Super Search

A web-based PDF search tool for searching across large collections of documents.

## Features

- Full-text search across PDF documents
- Page-level results with context snippets
- Password-protected access
- Resume-capable extraction (skip already processed files)
- Deployable to Azure Container Apps

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure

Copy `config.example.json` to `config.json` and set your PDF source folder:

```json
{
  "source_folder": "C:/path/to/your/pdfs",
  "index_folder": "./index",
  "file_extensions": [".pdf"],
  "port": 5000
}
```

### 3. Extract text from PDFs

```bash
python extract.py
```

This creates an `index/` folder with extracted text from all PDFs.

### 4. Run the web server

```bash
python run_web.py
```

Open http://localhost:5000 in your browser.

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `APP_PASSWORD` | Password for web access | (none - open access) |
| `FLASK_SECRET_KEY` | Session encryption key | dev key |
| `INDEX_FOLDER` | Location of extracted text | `./index` |

### config.json

| Field | Description |
|-------|-------------|
| `source_folder` | Path to PDF files |
| `index_folder` | Where to store extracted text |
| `file_extensions` | File types to process |
| `port` | Web server port |

## CLI Commands

### Extract text

```bash
# Extract using config.json settings
python extract.py

# Override source folder
python extract.py --source /path/to/pdfs

# Force re-extract all files
python extract.py --reindex
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Search interface |
| `/api/search?q=<query>&page=1` | GET | Search documents |
| `/api/stats` | GET | Index statistics |

### Search Response

```json
{
  "query": "emissions testing",
  "total_matches": 47,
  "documents": 23,
  "results": [
    {
      "filename": "Ford-S-00000001_0001.pdf",
      "filepath": "PDF001/Ford-S-00000001_0001.pdf",
      "page": 3,
      "context": "...conducted <mark>emissions testing</mark> in accordance...",
      "match_count": 2
    }
  ],
  "page": 1,
  "has_more": true
}
```

## Deployment

See [DEPLOYMENT.md](DEPLOYMENT.md) for Azure deployment instructions.

## Reusing for Other PDF Collections

1. Clone this repository
2. Create `config.json` pointing to your PDF folder
3. Run `python extract.py` to build the index
4. Run the server or deploy to Azure

The tool is designed to be reusable for any collection of PDFs.

## License

Proprietary - Thompsons Solicitors
