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

ENV DAGSTER_HOME=/opt/dagster_home
RUN mkdir -p $DAGSTER_HOME

EXPOSE 3000

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-m", "orchestration.dagster.dvf_dagster.definitions"]