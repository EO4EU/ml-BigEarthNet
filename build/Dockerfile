FROM registry.apps.eo4eu.eu/eo4eu/eo4eu-inference-server/bigearthnet/eo4eu-wrapper-bigearthnet-include:latest as Include
FROM ubuntu
ENV FLASK_APP=SSL-Image2Feature
COPY requirements.txt /app/requirements.txt
COPY --from=Include /app /app
WORKDIR /app 
RUN apt-get update && apt-get install python3 python3-pip ffmpeg libsm6 libxext6 software-properties-common wget -y
RUN mkdir -p /root/.config/pip
COPY pip.conf /root/.config/pip/pip.conf
RUN add-apt-repository ppa:ubuntugis/ppa
RUN apt-get install -y gdal-bin
RUN apt-get install -y libgdal-dev
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal
RUN GDAL_VERSION=$(ogrinfo --version | grep -oP '(\d+\.\d+\.\d+)') && pip install GDAL==$GDAL_VERSION
RUN pip install -r requirements.txt
RUN wget https://step.esa.int/thirdparties/sen2cor/2.11.0/Sen2Cor-02.11.00-Linux64.run
RUN chmod +x Sen2Cor-02.11.00-Linux64.run
RUN ./Sen2Cor-02.11.00-Linux64.run
COPY . /app
EXPOSE 8080
ENTRYPOINT [ "flask" ]
CMD [ "run","--port","8080","--host","0.0.0.0" ]