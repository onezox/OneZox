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
COPY aggregator/ aggregator/
# Every new top-level module directory needs its own COPY line above —
# Step F found this list silently stale (it hadn't been touched since
# Step B) and this line was itself missing when Step G first added
# aggregator/, caught the same way. main.py's verify_modules_present()
# boot-time smoke test is the SAFETY NET for the next time this list is
# forgotten (fails loud at startup instead of a silent bad image), not a
# substitute for keeping it current.
EXPOSE 8080
EXPOSE 50051
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
