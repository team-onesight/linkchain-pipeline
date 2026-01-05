FROM apache/airflow:3.1.2-python3.12

USER root
ENV PATH="/home/airflow/.local/bin:$PATH"
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY pyproject.toml .

COPY dbt_requirements.txt .
RUN uv venv /opt/airflow/dbt_venv && \
    uv pip install --python /opt/airflow/dbt_venv -r dbt_requirements.txt

RUN uv pip install --system \
    --extra-index-url https://download.pytorch.org/whl/cpu \
    torch torchvision

RUN uv pip install --system -r pyproject.toml

RUN apt-get update && apt-get install -y \
    mecab \
    libmecab-dev \
    && rm -rf /var/lib/apt/lists/*

RUN ln -s /etc/mecabrc /usr/local/etc/mecabrc

RUN playwright install-deps chromium

USER airflow
RUN playwright install chromium

RUN pip install transformers -U
RUN uv pip install --no-cache "spacy>=3.7.0,<4.0.0"
RUN python -m spacy download en_core_web_sm
