# syntax=docker/dockerfile:1.27.1@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e
# Thin Hive Metastore image: official Hive 3.1.3 + S3A and JDBC jars. No configuration, no credentials.
ARG HIVE_IMAGE=apache/hive:3.1.3@sha256:d102ba29ad07e93c303894896203a80b903c0001d80221f1cb9fea92dcac06e4
FROM ${HIVE_IMAGE}
USER root
# hadoop-aws MUST equal the image's hadoop-common 3.1.0 (E17); SDK = the version hadoop 3.1.0 was built with
ADD --chmod=644 --checksum=sha256:a18508b9348af095ea41301e439354dbd449e304ac44c6885b2b4fe78de88126 \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.0/hadoop-aws-3.1.0.jar /opt/hive/lib/
ADD --chmod=644 --checksum=sha256:faf78ac4880f56cf52791d84ec1068ce7c66acc4295d580a726104b734c01fcd \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar /opt/hive/lib/
# Exactly one JDBC driver: 42.7.4 (SCRAM-SHA-256 for Postgres 16); remove the image's 9.4 driver (E21)
ADD --chmod=644 --checksum=sha256:188976721ead8e8627eb6d8389d500dccc0c9bebd885268a3047180274a6031e \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/hive/lib/
RUN rm -f /opt/hive/lib/postgresql-9.4.1208.jre7.jar
USER hive
