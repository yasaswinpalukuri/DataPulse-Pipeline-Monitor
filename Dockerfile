FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestion/ ingestion/
COPY warehouse/ warehouse/

# One-shot batch: run one logical day and exit. Scheduling is cron's job
# (see deploy/crontab), not a loop inside the container.
ENTRYPOINT ["python", "-m", "ingestion.run_once"]
