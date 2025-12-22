# Linkchain Pipeline

## Structure

```
├── Dockerfile # Custom Airflow Dockerfile
├── README.md
├── ansible # Deployment automation scripts with Ansible
├── docker-compose-prod.yaml # Production Docker Compose file
├── docker-compose.yaml # Development Docker Compose file
├── .env.local # Local environment variables
├── .env.prod # Production environment variables
├── linkchain-pipeline.iml
├── pyproject.toml
├── scripts # Utility scripts for setup and management
├── src # Source code for airflow pipelines
│   ├── config
│       ├── airflow.cfg # Airflow config file
│       └── airflow_prod.cfg # Production Airflow config file
│   ├── dags
│   ├── logs
│   └── plugins
├── tests
└── uv.lock
```

## Setup Local Airflow Environment
```shell
uv sync
sh ./scripts/local/init-local-airflow.sh 
```
