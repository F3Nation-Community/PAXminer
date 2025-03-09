gcloud auth configure-docker us-central1-docker.pkg.dev
    
docker build . --platform linux/amd64 --tag us-central1-docker.pkg.dev/f3slackbot/paxminer-db-management/charts-publisher:latest

docker push us-central1-docker.pkg.dev/f3slackbot/paxminer-db-management/charts-publisher:latest