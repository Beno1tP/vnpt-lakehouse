# syntax=docker/dockerfile:1.27.1@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e
# Task 2 job image: the Task 1 thin Spark image + job code + versioned checks (shared-base pattern).
# Build context = jobs/ only, plus a named context "tests" (tests/), so nothing else from the repo (secrets, data)
# is ever sent to the builder. Build: make k8s-job-image
ARG BASE_IMAGE=vnpt-lake-spark:3.5.9-iceberg1.11.0-r1
FROM ${BASE_IMAGE}
COPY --chown=185:185 *.py /opt/jobs/
COPY --from=tests --chown=185:185 expected/ /opt/tests/expected/
COPY --from=tests --chown=185:185 sql/ /opt/tests/sql/
