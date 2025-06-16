FROM bitnami/spark:3.5.0

USER root
RUN apt-get update && apt-get install -y python3-pip && \
    pip3 install kafka-python pyspark pandas duckdb

WORKDIR /app
COPY ./scripts /app/scripts
COPY ./data/cleaned/LTM_Data_Cleaned_final.csv /app/data/

CMD ["spark-submit", "--master", "local[*]", "/app/scripts/datastream.py"]
