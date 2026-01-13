FROM registry.apps.eo4eu.eu/eo4eu/eo4eu-inference-server/bigearthnet/eo4eu-wrapper-bigearthnet-include:latest as Include
FROM dhi.io/python:3.13
ENV FLASK_APP=SSL-Image2Feature
RUN apt-get update && \
    apt-get install \
    ffmpeg \
    libsm6 \
    libxext6 \
    libgdal-dev \
    -y
RUN groupadd -g 1000 eo4eu && \
    useradd -u 1000 -g 1000 -m eo4eu
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
RUN pip install torch --index-url https://download.pytorch.org/whl/cpu
COPY --from=Include /app /app 
EXPOSE 8080
RUN chown -R eo4eu:eo4eu /app
USER eo4eu
ENTRYPOINT [ "flask" ]
CMD [ "run","--port","8080","--host","0.0.0.0"]