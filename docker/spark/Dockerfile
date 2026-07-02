FROM apache/spark:3.5.3

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip \
    python3-dev \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir \
    minio==7.1 \
    pandas==2.0 \
    numpy==1.24 \
    pyarrow==14.0 \
    kafka-python==2.0

# Hadoop AWS jar for S3 compatibility
RUN wget -q -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Kafka Streaming jars
RUN wget -q -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

USER spark
