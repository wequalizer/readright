FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY pyproject.toml ./
RUN pip install --no-cache-dir ".[api]" defusedxml

# Copy application
COPY src/ src/
COPY api/ api/

# Non-root user
RUN useradd -r -s /bin/false readright
USER readright

EXPOSE 8500

# Gunicorn with uvicorn workers for production
CMD ["python", "-m", "gunicorn", "api.main:app", \
     "--worker-class", "uvicorn.workers.UvicornWorker", \
     "--workers", "2", \
     "--bind", "0.0.0.0:8500", \
     "--timeout", "30", \
     "--max-requests", "1000", \
     "--max-requests-jitter", "50"]
