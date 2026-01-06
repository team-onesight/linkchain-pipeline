FROM apache/airflow:3.1.2-python3.12

USER root
ENV PATH="/home/airflow/.local/bin:$PATH"
ENV UV_LINK_MODE=copy

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN apt-get update && apt-get install -y \
    mecab \
    libmecab-dev \
    git \
    && rm -rf /var/lib/apt/lists/*
RUN ln -s /etc/mecabrc /usr/local/etc/mecabrc

RUN uv pip install --system playwright
RUN playwright install-deps chromium

COPY dbt_requirements.txt .
RUN uv venv /opt/airflow/dbt_venv && \
    uv pip install --python /opt/airflow/dbt_venv -r dbt_requirements.txt


RUN uv pip install --system \
    --extra-index-url https://download.pytorch.org/whl/cpu \
    torch torchvision

RUN pip install transformers -U
RUN uv pip install --no-cache "spacy>=3.7.0,<4.0.0"
RUN python -m spacy download en_core_web_sm


COPY pyproject.toml .
RUN uv pip install --system -r pyproject.toml

RUN chown -R airflow:root /opt/airflow/dbt_venv /opt/airflow/dbt_requirements.txt /opt/airflow/pyproject.toml && \
    chmod -R 755 /opt/airflow/dbt_venv
USER airflow
RUN playwright install chromium
