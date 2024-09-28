FROM python:3.9-slim

COPY requirements.txt /app/
RUN pip install -r /app/requirements.txt

# solution scripts
COPY solution/data_reader.py /app/data_reader.py
COPY solution/data_processor.py /app/data_processor.py

WORKDIR /app

# processing
CMD ["bash", "-c", "python3 data_reader.py && python data_processor.py"]