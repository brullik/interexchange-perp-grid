FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN groupadd --system ipeg && useradd --system --gid ipeg --home /app ipeg

COPY pyproject.toml README.md requirements.lock /app/
COPY src /app/src
RUN python -m pip install -r requirements.lock \
    && python -m pip install . --no-deps --no-build-isolation

COPY config /app/config
RUN mkdir -p /app/state /app/data /app/logs && chown -R ipeg:ipeg /app

USER ipeg

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD interexchange-grid health --config /app/config/defaults.yaml || exit 1

CMD ["interexchange-grid", "run", "--config", "/app/config/defaults.yaml"]
