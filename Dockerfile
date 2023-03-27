FROM python:3.10-slim

RUN apt-get update
RUN apt -y install postgresql
RUN pip install psycopg2-binary

WORKDIR /app

COPY . .

CMD ["python", "-u", "db_replication_script.py"]
