# balance-tracker-postgrest in the rewriter stack (compose.yml beside it): upstream.py on the
# image AIDEV already runs its external-service relays from.
FROM python:3.12-alpine@sha256:c5ba3deef00c7758561a0f03c26f44f10589263d40e732b8748e0d1f38f6dd89

COPY upstream.py /upstream.py
USER nobody
EXPOSE 3000 3001
CMD ["python3", "-u", "/upstream.py"]
