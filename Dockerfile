FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV KAFKA_VERSION=3.7.2
ENV SCALA_VERSION=2.13
ENV KAFKA_HOME=/opt/kafka

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    netcat-traditional \
    openjdk-17-jre-headless \
    postgresql \
    postgresql-contrib \
    procps \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" -o /tmp/kafka.tgz \
    && tar -xzf /tmp/kafka.tgz -C /opt \
    && mv "/opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" "${KAFKA_HOME}" \
    && rm -f /tmp/kafka.tgz

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY src /app/src
COPY scripts /app/scripts
COPY sql /app/sql
RUN chmod +x /app/scripts/start_all_in_one.sh
ENV PYTHONPATH=/app/src
CMD ["/app/scripts/start_all_in_one.sh"]
