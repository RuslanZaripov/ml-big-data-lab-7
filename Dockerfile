FROM apache/spark:3.5.5-scala2.12-java11-python3-r-ubuntu

ARG SPARK_JAR_PATH=/opt/spark/jars/
WORKDIR /app

USER 0:0

ADD https://repo1.maven.org/maven2/com/clickhouse/spark/clickhouse-spark-runtime-3.5_2.12/0.8.1/clickhouse-spark-runtime-3.5_2.12-0.8.1.jar ${SPARK_JAR_PATH}
ADD https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.8.5/clickhouse-jdbc-0.8.5-all.jar ${SPARK_JAR_PATH}

COPY requirements.txt .
COPY src ./src
COPY conf ./conf

RUN pip install --no-cache-dir -r requirements.txt

COPY target/scala-2.12/assemblyApp.jar target/scala-2.12/

COPY entrypoint.sh .
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT [ "/app/entrypoint.sh" ]