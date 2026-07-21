FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml ./
COPY cex_mirror ./cex_mirror

RUN pip install --no-cache-dir .

# config.yaml is mounted at runtime; JWT comes from the environment.
ENTRYPOINT ["python", "-m", "cex_mirror", "/app/config.yaml"]
