# syntax=docker/dockerfile:1
# Task 5 step 5.1: thin Superset image = the official lean image + the StarRocks SQLAlchemy dialect + the Postgres
# driver for the metadata DB. Built by CI (images.yml), stored in GHCR, deployed by digest (D5). Superset 6.1.0's
# Dockerfile: venv /app/.venv (on PATH), packages via uv, final user superset, no postgres extra in the lean image.
FROM apache/superset:6.1.0@sha256:16b50bbef6648912a79e3293d418fefa743ce555d36c5af6b85d859119ed7f88
USER root
COPY requirements.txt /tmp/vnpt-requirements.txt
# D26: every package Superset already has is a constraint (its exact installed version), so adding the dialect can
# never upgrade or downgrade Superset's own dependencies; uv pip check fails the build on any broken requirement.
RUN . /app/.venv/bin/activate \
 && uv pip freeze --exclude-editable > /tmp/superset-constraints.txt \
 && uv pip install --no-cache -r /tmp/vnpt-requirements.txt -c /tmp/superset-constraints.txt \
 && uv pip check \
 && rm /tmp/vnpt-requirements.txt /tmp/superset-constraints.txt
USER superset
