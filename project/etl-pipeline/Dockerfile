FROM apache/superset:latest

USER root

# Cài psycopg2-binary bằng python trong virtualenv sẵn có
RUN /app/.venv/bin/python -m ensurepip && \
    /app/.venv/bin/python -m pip install --no-cache-dir psycopg2-binary

USER superset
