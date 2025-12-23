FROM apache/airflow:3.1.2-python3.12

USER root

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY --chown=airflow:0 pyproject.toml .

RUN uv pip compile pyproject.toml -o requirements.txt && \
    uv pip install --python $(which python) -r requirements.txt

USER airflow
