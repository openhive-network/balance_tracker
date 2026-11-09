# balance-tracker-postgrest in the rewriter stack (compose.yml beside it): upstream.py on the
# image AIDEV already runs its external-service relays from.
FROM python:3.12-alpine@sha256:e064671ca452259bab50e8e2c7bd896e39e9a343bb06ca25234adf6fe18d0fc7

COPY upstream.py /upstream.py
USER nobody
EXPOSE 3000 3001
CMD ["python3", "-u", "/upstream.py"]
