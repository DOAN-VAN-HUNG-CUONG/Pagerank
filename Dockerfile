# =============================================================================
# PageRank — reproducible container
# =============================================================================
# Python 3.11 is used deliberately: mrjob 0.7.x depends on `distutils`, which
# Python 3.12 removed, so 3.11 lets the image run EVERY Python implementation
# (core, mrjob core + MapReduce, Hadoop Streaming local) and the full test suite
# except the PySpark test (PySpark is large and normally runs on a cluster; see
# the comment near the pip install below to enable it).
# -----------------------------------------------------------------------------
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/python

WORKDIR /app

# `make` for the convenience targets; bash/coreutils (sort, mktemp) needed by the
# Hadoop Streaming local simulation are already present in the Debian base image.
RUN apt-get update \
    && apt-get install -y --no-install-recommends make \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies first to leverage Docker layer caching.
COPY requirements.txt requirements-dev.txt ./
# Tooling + test dependencies. PySpark is intentionally omitted to keep the image
# small and the build fast; to run the PySpark implementation inside the
# container, add `pyspark` here and install a JRE (e.g. `default-jre-headless`).
RUN pip install --no-cache-dir -r requirements.txt pytest ruff scipy mrjob

# Copy the project (see .dockerignore for what is excluded).
COPY . .

# Default: run the fast test suite (the spark-marked test is skipped because
# spark-submit is not installed in this image).
CMD ["pytest", "-m", "not spark", "-q"]
