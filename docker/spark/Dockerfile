ARG SPARK_IMAGE=apache/spark:3.5.9-scala2.12-java17-python3-ubuntu
FROM ${SPARK_IMAGE}
ARG ICEBERG_VERSION=1.11.0
USER root
# 1) Slow-changing layers first (cached between code edits)
RUN pip install --no-cache-dir pandas==2.2.3 openpyxl==3.1.5 pyarrow==17.0.0
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
RUN chmod 644 /opt/spark/jars/*.jar /opt/spark/conf/spark-defaults.conf
# 2) Job code is bind-mounted at /opt/jobs (Task 1) and COPY'd later for K8s (Task 2)
USER spark
