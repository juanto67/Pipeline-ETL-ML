FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src

ENTRYPOINT ["sh", "-c", "python src/etl/extract.py && python src/etl/transform.py  && python src/etl/load.py"]
