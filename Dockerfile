FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir openpyxl
COPY src/ ./src/
COPY dags/ ./dags/
COPY sql/ ./sql/
COPY tests/ ./tests/
CMD ["python3", "-m", "unittest", "discover", "-s", "tests", "-p", "test_*.py"]
