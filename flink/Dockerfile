FROM apache/flink:1.19.1-scala_2.12-java17

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip curl \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && pip3 install --no-cache-dir apache-flink==1.19.1 \
    && curl -L -o /opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar \
       https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar \
    && curl -L -o /opt/flink/lib/flink-connector-jdbc-3.2.0-1.19.jar \
       https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar \
    && curl -L -o /opt/flink/lib/postgresql-42.7.3.jar \
       https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER flink
