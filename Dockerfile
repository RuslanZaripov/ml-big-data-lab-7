FROM apache/spark-py:latest

ARG PYSPARK_JAR_PATH=/opt/spark/jars/
WORKDIR /app

USER 0:0

COPY requirements.txt .
COPY src ./src
COPY conf ./conf
COPY .env .

ADD https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.8.5/clickhouse-jdbc-0.8.5-all.jar ${PYSPARK_JAR_PATH}
ADD https://repo1.maven.org/maven2/com/clickhouse/spark/clickhouse-spark-runtime-3.5_2.12/0.8.1/clickhouse-spark-runtime-3.5_2.12-0.8.1.jar ${PYSPARK_JAR_PATH}

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT [ "/opt/spark/bin/spark-submit", "src/clusterize.py", "--numPartitions", "10" ]