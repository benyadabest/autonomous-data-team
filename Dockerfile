FROM python:3.10-slim

WORKDIR /app

COPY pyproject.toml /app/pyproject.toml
COPY README.md /app/README.md
COPY autonomous_data_team /app/autonomous_data_team

RUN pip install --no-cache-dir .

ENV BIND_HOST=0.0.0.0
ENV PORT=8000

EXPOSE 8000

CMD ["autonomous-data-team", "serve"]
