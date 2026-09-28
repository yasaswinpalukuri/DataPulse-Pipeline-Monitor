FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt requirements-api.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements-api.txt

# Only what the API needs: its own package + the shared Snowflake client.
COPY api/ api/
COPY warehouse/__init__.py warehouse/snowflake_client.py warehouse/

EXPOSE 8000
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
