# Super Search - Azure Deployment

## Access Details

**URL:** (Will be assigned after deployment)

**Password:** Set via `APP_PASSWORD` environment variable in Azure Container Apps

## Azure Infrastructure

| Resource | Value |
|----------|-------|
| Container App | `super-search` |
| Resource Group | `thompsons-transcript-rg` |
| Environment | `thompsons-transcript-env` |
| Region | UK South |
| ACR | `thompsonstranscript.azurecr.io` |

## Initial Setup (One-time)

### 1. Create Container App

```bash
az containerapp create \
  --name super-search \
  --resource-group thompsons-transcript-rg \
  --environment thompsons-transcript-env \
  --image thompsonstranscript.azurecr.io/super-search:latest \
  --target-port 5000 \
  --ingress external \
  --registry-server thompsonstranscript.azurecr.io \
  --env-vars APP_PASSWORD=<your-password> FLASK_SECRET_KEY=<random-secret>
```

### 2. Add GitHub Secret

Add `AZURE_CREDENTIALS` secret to the GitHub repository (same as Records repo).

## Deployment

The app automatically deploys when changes are pushed to the `main` branch via GitHub Actions.

To check deployment status: https://github.com/benlegalaitech/Thompsons-Super-Search/actions

## Manual Deployment

```bash
# Login to Azure
az login

# Login to ACR
az acr login --name thompsonstranscript

# Build and push
docker build -t thompsonstranscript.azurecr.io/super-search:latest .
docker push thompsonstranscript.azurecr.io/super-search:latest

# Update container app
az containerapp update \
  --name super-search \
  --resource-group thompsons-transcript-rg \
  --image thompsonstranscript.azurecr.io/super-search:latest
```

## Index Data

The extracted text index must be included in the Docker image. Before deploying:

1. Run extraction locally: `python extract.py`
2. Ensure `index/` folder is populated
3. Build Docker image (which copies `index/` into the container)
4. Push to ACR

**Note:** For very large indexes, consider using Azure Blob Storage instead of baking into the image.

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `APP_PASSWORD` | Yes | Password for web access |
| `FLASK_SECRET_KEY` | Yes | Session encryption key |
| `INDEX_FOLDER` | No | Defaults to `./index` |

## Monitoring

View logs in Azure Portal:
1. Go to Container Apps > super-search
2. Select "Log stream" or "Logs"

Or via CLI:
```bash
az containerapp logs show \
  --name super-search \
  --resource-group thompsons-transcript-rg \
  --follow
```
