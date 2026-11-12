# balance-tracker-postgrest in the rewriter stack (compose.yml beside it): upstream.py on the
# image AIDEV already runs its external-service relays from.
FROM python:3.15-alpine@sha256:0f9552bb28711333da16915ec146f5e6bb44a7fdac9ced15dc17c2e0206346dd

COPY upstream.py /upstream.py
USER nobody
EXPOSE 3000 3001
CMD ["python3", "-u", "/upstream.py"]
