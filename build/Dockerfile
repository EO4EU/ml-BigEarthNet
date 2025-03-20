FROM registry.apps.eo4eu.eu/eo4eu/eo4eu-inference-server/bigearthnet/eo4eu-wrapper-bigearthnet-include:latest as Include
FROM python:3.13.1-bookworm
ENV FLASK_APP=SSL-Image2Feature
RUN apt-get update && \
    apt-get install \
    ffmpeg \
    libsm6 \
    libxext6 \
    libgdal-dev \
    -y
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
COPY --from=Include /app /app 
EXPOSE 8080
ENTRYPOINT [ "flask" ]
CMD [ "run","--port","8080","--host","0.0.0.0" ]