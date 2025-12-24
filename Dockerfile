FROM apache/airflow:3.1.2-python3.12

USER root
ENV PATH="/home/airflow/.local/bin:$PATH"
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY pyproject.toml .


RUN uv pip install --system -r pyproject.toml

RUN playwright install-deps chromium

USER airflow
RUN playwright install chromium

