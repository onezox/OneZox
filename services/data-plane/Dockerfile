FROM docker.io/library/python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
COPY generated/ generated/
COPY planner/ planner/
COPY scheduler/ scheduler/
COPY working_memory/ working_memory/
COPY request_log/ request_log/
EXPOSE 8080
EXPOSE 50051
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
