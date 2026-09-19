FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /opt/dvf-etl-project

COPY requirements-dagster.txt .
RUN pip install --no-cache-dir -r requirements-dagster.txt

COPY . .

# RETL0-50: DAGSTER_HOME lives inside the volume-mounted repo directory
# (./:/opt/dvf-etl-project in docker-compose.yml), not a path baked into
# the image. That's what lets the separate dagster-daemon service share
# the exact same run/event/schedule storage as this webserver container --
# both mount the same host directory, so Dagster's default SQLite-backed
# storage there is really shared between them, the same officially
# documented pattern Dagster itself uses for webserver+daemon docker-compose
# deployments. Not created here with mkdir: that would only create a
# directory inside the image, which the volume mount replaces at container
# start anyway -- Dagster creates DAGSTER_HOME itself on first run if it
# doesn't already exist on the shared volume.
ENV DAGSTER_HOME=/opt/dvf-etl-project/.dagster_home

EXPOSE 3000

CMD ["sh", "-c", "mkdir -p $DAGSTER_HOME && dagster-webserver -h 0.0.0.0 -p 3000 -m orchestration.dagster.dvf_dagster.definitions"]