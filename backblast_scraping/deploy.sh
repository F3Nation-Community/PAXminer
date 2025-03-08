gcloud auth configure-docker us-central1-docker.pkg.dev
    
docker build . --platform linux/amd64 --tag us-central1-docker.pkg.dev/f3slackbot/paxminer-db-management/paxminer-scraping:latest

docker push us-central1-docker.pkg.dev/f3slackbot/paxminer-db-management/paxminer-scraping:latest