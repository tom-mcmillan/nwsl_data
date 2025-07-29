# NWSL MCP Server - Cloud Build + Cloud Run Setup

## 🚀 Quick Setup Steps

You now have all the files needed for a professional CI/CD pipeline. Here's how to set it up:

### 1. Push to GitHub Repository

First, make sure your code is in a GitHub repository:

```bash
# If not already a git repo:
git init
git add .
git commit -m "Initial NWSL MCP server setup"

# Push to GitHub (replace with your repo URL)
git remote add origin https://github.com/yourusername/nwsl_data.git
git branch -M main
git push -u origin main
```

### 2. Connect GitHub to Cloud Build

In Google Cloud Console:

1. Go to **Cloud Build > Triggers**
2. Click **"Connect Repository"**
3. Select **GitHub** as source
4. Authenticate and select your `nwsl_data` repository
5. Click **"Connect"**

### 3. Create Build Trigger

Still in Cloud Build > Triggers:

1. Click **"Create Trigger"**
2. Configure:
   - **Name**: `nwsl-mcp-server-deploy`
   - **Event**: Push to a branch
   - **Source**: Your connected repository
   - **Branch**: `^main$`
   - **Configuration**: Cloud Build configuration file
   - **Location**: `/cloudbuild.yaml`
3. Click **"Create"**

### 4. Grant Cloud Build Permissions

```bash
# Get your project number
export PROJECT_NUMBER=$(gcloud projects describe nwsl-data --format="value(projectNumber)")

# Grant Cloud Run permissions to Cloud Build
gcloud projects add-iam-policy-binding nwsl-data \
    --member="serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com" \
    --role="roles/run.admin"

gcloud iam service-accounts add-iam-policy-binding \
    ${PROJECT_NUMBER}-compute@developer.gserviceaccount.com \
    --member="serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com" \
    --role="roles/iam.serviceAccountUser"
```

## 🔄 How It Works

Once set up, every push to your `main` branch will:

1. **Trigger Cloud Build** - Automatically starts the build process
2. **Build Docker Image** - Creates containerized version of your MCP server
3. **Push to Registry** - Stores the image in Google Container Registry
4. **Deploy to Cloud Run** - Updates your live service with the new version

## 📋 Files Created

- `cloudbuild.yaml` - Build pipeline configuration
- `Dockerfile` - Container definition with security best practices
- `.gcloudignore` - Excludes unnecessary files from builds
- `setup-deployment.md` - This setup guide

## 🌐 Your Live URL

After the first successful deployment, your NWSL MCP server will be available at:

```
https://nwsl-mcp-server-<hash>-uc.a.run.app
```

**For OpenAI Playground**, use:
```
https://nwsl-mcp-server-<hash>-uc.a.run.app/mcp
```

## 🔧 Manual Deploy (Optional)

If you want to deploy manually first:

```bash
# Build and deploy
gcloud builds submit --config cloudbuild.yaml .

# Or just deploy current code
gcloud run deploy nwsl-mcp-server \
  --source . \
  --region us-central1 \
  --allow-unauthenticated \
  --port 8000
```

## 📊 Monitoring

- **Build History**: Cloud Build > History
- **Service Logs**: Cloud Run > nwsl-mcp-server > Logs
- **Metrics**: Cloud Run > nwsl-mcp-server > Metrics

## 🔄 Updates

To update your server:
1. Make code changes
2. Commit and push to `main` branch
3. Cloud Build automatically rebuilds and redeploys
4. New version is live in ~3-5 minutes

Your NWSL MCP server now has enterprise-grade CI/CD! 🏆