FROM python:3.12-slim

WORKDIR /app
COPY requirements-dashboard.txt ./
RUN pip install --no-cache-dir -r requirements-dashboard.txt
COPY dashboard/ dashboard/

EXPOSE 8501
# PYTHONPATH so `from dashboard import api_client` resolves from /app.
ENV PYTHONPATH=/app
CMD ["streamlit", "run", "dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true", "--browser.gatherUsageStats=false"]
