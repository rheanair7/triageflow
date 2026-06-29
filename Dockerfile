FROM python:3.11-slim
WORKDIR /app
COPY backend.py index.html requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir -p /app/data
EXPOSE 8000
CMD ["uvicorn", "backend:app", "--host", "0.0.0.0", "--port", "8000"]
