FROM python:3.12-slim

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install CPU-only PyTorch first (avoids pulling ~2 GB of CUDA libs)
RUN pip install --no-cache-dir torch torchvision torchaudio \
    --index-url https://download.pytorch.org/whl/cpu

# Install Python dependencies (cached layer)
COPY pyproject.toml README.md ./
RUN pip install --no-cache-dir .

# Copy source
COPY . .

# Re-install with source so entry-points resolve
RUN pip install --no-cache-dir -e .

EXPOSE 8080

# Default: run the orchestrator
CMD ["python", "-m", "apps.orchestrator.main"]
