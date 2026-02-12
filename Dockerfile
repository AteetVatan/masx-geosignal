FROM python:3.12-slim

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY pyproject.toml ./
RUN pip install --no-cache-dir -e ".[dev]"

# Copy source
COPY . .

# Install the package
RUN pip install --no-cache-dir -e .

# Default: run the orchestrator
CMD ["python", "-m", "apps.orchestrator.main"]
