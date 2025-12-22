FROM apache/airflow:3.1.4-python3.12

USER root

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY pyproject.toml .

RUN uv pip install --system -r pyproject.toml

USER airflow
