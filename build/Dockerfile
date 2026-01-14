FROM registry.apps.eo4eu.eu/eo4eu/eo4eu-inference-server/bigearthnet/eo4eu-wrapper-bigearthnet-include:latest as Include
FROM python:3.13-slim-trixie AS build

ENV FLASK_APP=SSL-Image2Feature

RUN apt-get update && \
    apt-get install -y \
        ffmpeg \
        libsm6 \
        libxext6 \
        libgdal-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu

COPY --from=Include /app /app

RUN groupadd -g 1000 eo4eu && \
    useradd -u 1000 -g 1000 -m eo4eu && \
    chown -R eo4eu:eo4eu /app


FROM registry.apps.eo4eu.eu/eo4eu/eo4eu-inference-server/bigearthnet/python:3.13

ENV FLASK_APP=SSL-Image2Feature
WORKDIR /app

COPY --from=build /app /app
COPY --from=build /usr/local /usr/local
COPY --from=build /usr/lib/x86_64-linux-gnu /usr/lib/x86_64-linux-gnu/
COPY --from=build /usr/bin/ffmpeg /usr/bin/ffmpeg

EXPOSE 8080

USER 1000
ENTRYPOINT ["flask"]
CMD ["run", "--port", "8080", "--host", "0.0.0.0"]
