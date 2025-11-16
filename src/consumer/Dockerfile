FROM flink:1.19.0-scala_2.12-java11

USER root

WORKDIR /opt/flink

# Install Python and pip (for PyFlink jobs and extra deps)
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-venv ca-certificates && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Avoid entrypoint noise about a missing /config.yaml when running CLI containers
RUN touch /config.yaml

# Install Python dependencies for the Flink job
COPY requirements.txt /opt/flink/requirements.txt
RUN python3 -m pip install -r /opt/flink/requirements.txt

# Copy connector/driver JARs into Flink lib directory
COPY jars/*.jar /opt/flink/lib/

COPY jobs /opt/flink/jobs

USER flink