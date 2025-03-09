# PAXminer

PAXminer Backblast Scraping is a tool for automatically extracting and storing backblasts (workout summaries) from F3 Slack channels. It scrapes Slack channels, parses the backblast text, and stores the information in a database for analysis and reporting.

## Deployment to Google Cloud Run

This section outlines the steps for deploying PAXminer to Google Cloud Run. The deploy.sh file can be run to do the following 3 steps together.

### Deployment Steps

1.  **Authenticate Docker with Google Cloud:**

    ```bash
    gcloud auth configure-docker us-central1-docker.pkg.dev
    ```

2.  **Build the Docker Image:**

    ```bash
    docker build . --platform linux/amd64 --tag us-central1-docker.pkg.dev/f3slackbot/paxminer-db-management/paxminer-scraping:latest
    ```

3.  **Push the Docker Image to Google Container Registry:**

    ```bash
    docker push us-central1-docker.pkg.dev/f3slackbot/paxminer-db-management/paxminer-scraping:latest
    ```

## Running PAXminer Locally

This section describes how to run PAXminer locally for development or testing.

### Installation

1.  Clone the repository
2.  Create a virtual environment (recommended)
3.  Install the dependencies

### Configuration

1.  **Set Config Variables:**

    Set the following variables in your config/credentials.ini file:

    *   [host](http://_vscodecontentref_/1): The database host.
    *   [port](http://_vscodecontentref_/2): The database port (typically 3306).
    *   [user](http://_vscodecontentref_/3): The database user.
    *   [password](http://_vscodecontentref_/4): The database password.
    *   [db](http://_vscodecontentref_/5): The database name (e.g., `paxminer`).

### Execution

1.  **Run [PAXminer_Manual_Execution.py](http://_vscodecontentref_/6):**

    ```bash
    python PAXminer_Manual_Execution.py
    ```

    This will execute PAXminer for the region hardcoded in the script query on line 34.

2.  **Run [PAXMiner_Cloud_Run.py](http://_vscodecontentref_/7):**

    ```bash
    python PAXMiner_Cloud_Run.py A-Z
    ```

    This will execute PAXminer for all regions that match the regex `A-Z`. It also requires setting the config file as environment variables instead*.