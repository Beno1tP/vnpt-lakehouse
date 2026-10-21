# syntax=docker/dockerfile:1.27.1@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e
# Thin Spark runtime image: official Spark + job dependencies. No configuration, no credentials, no data.
ARG SPARK_IMAGE=apache/spark:3.5.9-scala2.12-java17-python3-ubuntu@sha256:f3d6eaa8bab8ec2e38f3c3918a5b2f8b253c95bcb19f9e87ad0e0cdacf9df2d5
FROM ${SPARK_IMAGE}
USER root
# Iceberg 1.11.0 jars (SHA-256 = values published on Maven Central)
ADD --chmod=644 --checksum=sha256:94b8e36fc329f0293d44ba9e01b784a56e9501affec1842d898144c51f6e486a \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.11.0/iceberg-spark-runtime-3.5_2.12-1.11.0.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:38f01da7e96850cdd05e6616d758b77b43314b712a8808e3f9a824d56976162f \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.11.0/iceberg-aws-bundle-1.11.0.jar /opt/spark/jars/
# Python dependencies for the image's own Python 3.10 (pinned)
COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir --disable-pip-version-check -r /tmp/requirements.txt && rm /tmp/requirements.txt
USER spark
