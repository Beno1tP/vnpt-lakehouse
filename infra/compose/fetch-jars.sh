#!/bin/sh
# One-shot: download HMS jars from Maven Central into /jars and verify SHA-1. Skips files already present.
set -eu
M=https://repo1.maven.org/maven2
cd /jars
get() {
  f=$(basename "$1")
  if [ -d "$f" ]; then echo "ERROR: $f is a directory (Docker created it for a missing bind-mount source). Remove it: rmdir infra/compose/jars/$f" >&2; exit 1; fi
  if [ -f "$f" ] && [ -s "$f" ]; then echo "have $f"; return 0; fi
  curl -fsSL -o "$f.part" "$M/$1"
  echo "$(curl -fsSL "$M/$1.sha1" | cut -c1-40)  $f.part" | sha1sum -c -
  mv "$f.part" "$f"
}
get "org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"
get "com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"
get "org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar"
ls -l /jars
