FROM apache/airflow:3.1.8

USER root

# Install system dependencies for Playwright
RUN apt-get update && apt-get install -y \
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libdbus-1-3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    libatspi2.0-0 \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
COPY pyproject.toml uv.lock README.md /opt/airflow/project/
RUN pip install --no-cache-dir \
    playwright \
    pyyaml \
    requests

# Install Playwright browser
RUN playwright install chromium

# Install the project package
COPY src/ /opt/airflow/project/src/
COPY config/ /opt/airflow/project/config/
WORKDIR /opt/airflow/project
RUN pip install --no-cache-dir .