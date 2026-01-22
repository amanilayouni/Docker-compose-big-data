
FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python requests
COPY weather_producer.py /app/weather_producer.py
CMD ["python","/app/weather_producer.py"]