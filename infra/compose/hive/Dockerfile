ARG HIVE_IMAGE=apache/hive:4.0.1
FROM ${HIVE_IMAGE}
ARG HADOOP_AWS_VERSION
ARG AWS_SDK_BUNDLE_VERSION
ARG PG_JDBC_VERSION
USER root
# Postgres JDBC driver (expected at /opt/hive/lib/postgres.jar by the image entrypoint)
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar /opt/hive/lib/postgres.jar
# S3A support so the Metastore can create database/table dirs on MinIO
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar /opt/hive/lib/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar /opt/hive/lib/
COPY core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
RUN chmod 644 /opt/hive/lib/*.jar /opt/hadoop/etc/hadoop/core-site.xml
USER hive
