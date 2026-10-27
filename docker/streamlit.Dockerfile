FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app
COPY requirements-dashboard.txt /app/requirements-dashboard.txt
RUN pip install --no-cache-dir -r /app/requirements-dashboard.txt

COPY .streamlit /app/.streamlit
COPY config /app/config
COPY dashboard /app/dashboard

EXPOSE 8501
CMD ["streamlit", "run", "dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501"]
