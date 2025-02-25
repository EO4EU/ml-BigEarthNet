from flask import Flask, request, make_response
import uuid
import json
import sys
from cloudpathlib import CloudPath
from cloudpathlib import S3Client
import BigEarthNetLoader
import tritonclient.http as httpclient
import numpy as np
from kafka import KafkaProducer
import os
from kubernetes import client, config
import tarfile

import traceback

from io import BytesIO
import re

import subprocess

import tempfile

from osgeo import gdal

from pathlib import Path

import threading

import rasterio

import cv2

import zipfile



app = Flask(__name__)

Producer=KafkaProducer(bootstrap_servers='bootstrap.dev.wekeo.apps.eo4eu.eu:443',security_protocol='SSL',value_serializer=lambda v: json.dumps(v).encode('utf-8'),key_serializer=str.encode)

# This is the entry point for the SSL model from Image to Feature service.
# It will receive a message from the Kafka topic and then do the inference on the data.
# The result will be sent to the next service.
# The message received should be a json with the following fields:
# previous_component_end : A boolean that indicate if the previous component has finished.
# S3_bucket_desc : A json with the following fields:
# folder : The folder where the data is stored.
# The namespace of the configmap to read is the name of the pod.
# The name of the configmap to read is given by the URL.
# The configmap should have a field named jsonSuperviserRequest that is a json with the following fields:
# Topics : A json with the following fields:
# out : The name of the kafka topic to send the result.
# S3_bucket : A json with the following fields:
# aws_access_key_id : The access key id of the S3 bucket.
# aws_secret_access_key : The secret access key of the S3 bucket.
# s3-bucket_name : The name of the S3 bucket.
# region_name : The name of the region of the S3 bucket.
# endpoint_url : The endpoint url of the S3 bucket.
# ML : A json with the following fields:
# need-to-resize : A boolean that indicate if the data need to be resized.
@app.route('/<name>', methods=['POST'])
def ssl_image2Feature(name):
      """
      Endpoint function that receives a POST request with a name parameter.
      Reads a config map from a Kubernetes cluster and extracts relevant data from it.
      Downloads data from an S3 bucket and processes it using Sen2Cor.
      Uploads the processed data to another S3 bucket.
      """
      # TODO : Debugging message to remove in production.
      # Message received.
      response=None
      try:
            config.load_incluster_config()
            api_instance = client.CoreV1Api()
            configmap_name = str(name)
            configmap_namespace = 'test-bigearthnet'
            app.logger.warning('Namespace'+str(configmap_namespace))
            api_response = api_instance.read_namespaced_config_map(configmap_name, configmap_namespace)
            json_data_request = json.loads(request.data)
            json_data_configmap =json.loads(str(api_response.data['jsonSuperviserRequest']))      
            app.logger.warning('Reading json data request'+str(json_data_request))
            app.logger.warning('Reading json data configmap'+str(json_data_configmap))
            assert json_data_request['previous_component_end'] == 'True' or json_data_request['previous_component_end']
            kafka_out = json_data_configmap['Topics']["out"]
            s3_access_key = json_data_configmap['S3_bucket']['aws_access_key_id']
            s3_secret_key = json_data_configmap['S3_bucket']['aws_secret_access_key']
            s3_bucket_output = json_data_configmap['S3_bucket']['s3-bucket-name']
            s3_region = json_data_configmap['S3_bucket']['region_name']
            s3_region_endpoint = json_data_configmap['S3_bucket']['endpoint_url']
            NeedResize = json_data_configmap['ML']['need-to-resize']
            s3_path = json_data_request['S3_bucket_desc']['folder']
            s3_file = json_data_request['S3_bucket_desc'].get('filename',None)

            def threadentry():
                  app.logger.warning('All json data read')

                  clientS3 = S3Client(aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key,endpoint_url=s3_region_endpoint)
                  clientS3.set_as_default_client()

                  app.logger.warning('Client is ready')
                  lcp=[]
                  if s3_path == '':
                        cp = CloudPath("s3://"+s3_bucket_output+'/'+s3_file, client=clientS3)
                        app.logger.warning("path is s3://"+s3_bucket_output+'/'+s3_file)
                        with tempfile.TemporaryDirectory() as tempdir:
                              cp.download_to(tempdir)
                              with tempfile.TemporaryDirectory() as tempdir2:
                                    with zipfile.ZipFile(tempdir+"/"+cp.name, 'r') as zip_ref:
                                          zip_ref.extractall(tempdir2)
                                    cp = CloudPath("s3://"+s3_bucket_output+'/'+"ssl-unzip/", client=clientS3)
                                    cp.upload_from(tempdir2)
                  elif s3_file == None:
                        cp = CloudPath("s3://"+s3_bucket_output+'/'+s3_path, client=clientS3)
                        app.logger.warning("path is s3://"+s3_bucket_output+'/'+s3_path)
                        for folder in cp.iterdir():
                              pattern=r'.*\.zip$'
                              match = re.search(pattern,folder.name)
                              if match:
                                    with tempfile.TemporaryDirectory() as tempdir:
                                          folder.download_to(tempdir)
                                          with tempfile.TemporaryDirectory() as tempdir2:
                                                with zipfile.ZipFile(tempdir+"/"+folder.name, 'r') as zip_ref:
                                                      zip_ref.extractall(tempdir2)
                                                cpTemp=CloudPath("s3://"+s3_bucket_output+'/'+"ssl-unzip/", client=clientS3)
                                                lcp.append(cpTemp)
                                                cpTemp.upload_from(tempdir2)
                  else:
                        cp = CloudPath("s3://"+s3_bucket_output+'/'+s3_path+'/'+s3_file, client=clientS3)
                        app.logger.warning("path is s3://"+s3_bucket_output+'/'+s3_path+'/'+s3_file)
                  cpOutput = CloudPath("s3://"+s3_bucket_output+"/"+"SSL-Image2Feature-output/", client=clientS3)
                  for cp in lcp:
                        for folder in cp.iterdir():
                              pattern=r'MSIL1C..*\.SAFE$'
                              match = re.search(pattern,folder.name)
                              pattern2=r'MSIL2A..*\.SAFE$'
                              match2 = re.search(pattern2,folder.name)
                              with tempfile.TemporaryDirectory() as tempdir:
                                    if match:
                                          path=os.path.join(tempdir,folder.name)
                                          folder.download_to(path)
                                          app.logger.warning("folder path "+path)
                                          cmd=f'/app/Sen2Cor-02.11.00-Linux64/bin/L2A_Process --tif {path}'
                                          process = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                                          stdout, stderr = process.communicate()
                                          app.logger.warning('Communicating')
                                          output = stdout.decode('utf-8')
                                          error_output = stderr.decode('utf-8')
                                          if process.returncode == 0:
                                                app.logger.warning('Command executed successfully. Here is the output:')
                                                app.logger.warning(output)
                                          else:
                                                app.logger.warning('Command failed. Here is the output:')
                                                app.logger.warning(output)
                                                app.logger.warning('Here is the error:')
                                                app.logger.warning(error_output)
                                          cpTmp=Path(tempdir)
                                    elif match2:
                                          path=os.path.join(tempdir,folder.name)
                                          folder.download_to(path)
                                          cpTmp=Path(tempdir)
                                    if match or match2:
                                          for folderOutput in cpTmp.iterdir():
                                                pattern=r'.*MSIL2A.*\.SAFE$'
                                                match = re.search(pattern,folderOutput.name)
                                                if match:
                                                      app.logger.warning("folder output path "+str(folderOutput))

                                                      def TransformDataBG(path_src,path_dst,xoff,yoff,pixel):
                                                            def get_gdal_format(file_extension):
                                                                  """Get the corresponding GDAL format for a given file extension.

                                                                  Parameters:
                                                                  file_extension (str): The file extension for which to determine the GDAL format.

                                                                  Returns:
                                                                  str: The GDAL format name.
                                                                  """
                                                                  # A dictionary mapping file extensions to GDAL format names.
                                                                  # This is not an exhaustive list.
                                                                  formats = {
                                                                        '.tif': 'GTiff',
                                                                        '.tiff': 'GTiff',
                                                                        '.png': 'PNG',
                                                                        '.jpg': 'JPEG',
                                                                        '.jpeg': 'JPEG',
                                                                        '.gif': 'GIF',
                                                                        '.gsg': 'GSG',
                                                                        '.bmp': 'BMP',
                                                                        '.hdf': 'HDF4',
                                                                        # Add more formats here as needed
                                                                  }

                                                                  # The file extension should start with a '.', if not, prepend it.
                                                                  if not file_extension.startswith('.'):
                                                                        file_extension = '.' + file_extension

                                                                  # Get the corresponding format from the dictionary.
                                                                  gdal_format = formats.get(file_extension.lower())

                                                                  if gdal_format is None:
                                                                        raise ValueError(f"No corresponding GDAL format found for the given extension: {file_extension}")

                                                                  return gdal_format

                                                            src=gdal.Open(path_src)
                                                            file_extension = os.path.splitext(path_src)[1]
                                                            format = get_gdal_format(file_extension)
                                                            driver = gdal.GetDriverByName(format)
                                                            translate_options = gdal.TranslateOptions(format=format, srcWin=[xoff, yoff, pixel, pixel])
                                                            dst = gdal.Translate(path_dst, src, options=translate_options)
                                                            dst=None
                                                            src=None
                                                      with tempfile.TemporaryDirectory() as tempdirBen:
                                                            cpGranule=folderOutput/"GRANULE"
                                                            dicPath={}
                                                            for folderGranule in cpGranule.iterdir():
                                                                  app.logger.warning("granule path "+str(folderGranule))
                                                                  cpIMGData10m=folderGranule/"IMG_DATA"/"R10m"
                                                                  for image in cpIMGData10m.iterdir():
                                                                        pattern=r'.*_(.*)_10m\.tif$'
                                                                        match = re.search(pattern,image.name)
                                                                        app.logger.warning("image path "+str(image))
                                                                        if match:
                                                                              app.logger.warning("matched")
                                                                              matchedBand=match.group(1)
                                                                              app.logger.warning("matchedBand "+matchedBand)
                                                                              if matchedBand in ['B02','B03','B04','B08']:
                                                                                    app.logger.warning("matchedBand 10m "+matchedBand)
                                                                                    path_src=str(image)
                                                                                    path_dst=str(tempdirBen)+"/"+matchedBand+".tif"
                                                                                    app.logger.warning("path_src "+path_src)
                                                                                    app.logger.warning("path_dst "+path_dst)
                                                                                    TransformDataBG(path_src,path_dst,0,0,120)
                                                                                    dicPath[matchedBand]=path_dst
                                                                        else:
                                                                              app.logger.warning("not matched")
                                                                  cpIMGData20m=folderGranule/"IMG_DATA"/"R20m"
                                                                  for image in cpIMGData20m.iterdir():
                                                                        pattern=r'.*_(.*)_20m\.tif$'
                                                                        match = re.search(pattern,image.name)
                                                                        app.logger.warning("image path "+str(image))
                                                                        if match:
                                                                              app.logger.warning("matched")
                                                                              matchedBand=match.group(1)
                                                                              app.logger.warning("matchedBand "+matchedBand)
                                                                              if matchedBand in ['B05','B06','B07','B8A','B11','B12']:
                                                                                    app.logger.warning("matchedBand 20m "+matchedBand)
                                                                                    path_src=str(image)
                                                                                    path_dst=str(tempdirBen)+"/"+matchedBand+".tif"
                                                                                    app.logger.warning("path_src "+path_src)
                                                                                    app.logger.warning("path_dst "+path_dst)
                                                                                    TransformDataBG(path_src,path_dst,0,0,60)
                                                                                    dicPath[matchedBand]=path_dst
                                                                        else:
                                                                              app.logger.warning("not matched")
                                                                  BANDS_10M = [
                                                                              "B04",
                                                                              "B03",
                                                                              "B02",
                                                                              "B08",
                                                                              ]


                                                                  BANDS_20M = [
                                                                  "B05",
                                                                  "B06",
                                                                  "B07",
                                                                  "B8A",
                                                                  "B11",
                                                                  "B12",
                                                                  ]
                                                                  
                                                                  BANDS_ALL=BANDS_10M+BANDS_20M
                                                                  bands_data = []

                                                                  for band_name in BANDS_ALL:
                                                                        band_path = dicPath[band_name]
                                                                        with rasterio.open(band_path,driver="GTiff",sharing=False) as band_file:
                                                                              band_data   = band_file.read(1)  # open the tif image as a numpy array
                                                                              # Resize depending on the resolution
                                                                              if band_name in BANDS_20M:
                                                                                    # Carry out a bicubic interpolation (TUB does exactly this)
                                                                                    band_data = cv2.resize(band_data, dsize=(120, 120), interpolation=cv2.INTER_CUBIC)
                                                                                    # We have already ignored the 60M ones, and we keep the 10M ones intact
                                                                              #logging.info("appending")
                                                                              bands_data.append(band_data)
                                                                              app.logger.warning("band_name "+band_name)
                                                                              app.logger.warning("band_data shape "+str(band_data.shape))
                                                                        band_file.close()

                                                                  bands_data = np.stack(bands_data)
                                                                  bands_data = BigEarthNetLoader.normalize_bands(bands_data)
                                                                  data=np.expand_dims(bands_data.astype(np.float32),axis=0)
                                                                  result=doInference(data)
                                                                  outputPath=cpOutput.joinpath(folderOutput.name)
                                                                  with outputPath.open('w') as outputFile:
                                                                        json.dump(result, outputFile) 
                  app.logger.warning('Connecting to Kafka')
                  response_json ={
                  "previous_component_end": "True",
                  "S3_bucket_desc": {
                        "folder": "SSL-Image2Feature-output","filename": ""
                  },
                  "dataset_meta_info": {
                        "meta_information": "json"
                  }}
                  Producer.send(kafka_out,key='key',value=response_json)
                  Producer.flush()
            thread = threading.Thread(target=threadentry)
            thread.start()
            response = make_response({
                        "msg": "Started the process"
                        })
      except Exception as e: 
            app.logger.warning('Got exception '+str(e))
            app.logger.warning(traceback.format_exc())
            app.logger.warning('So we are ignoring the message')
            # HTTP answer that the message is malformed. This message will then be discarded only the fact that a sucess return code is returned is important.
            response = make_response({
            "msg": "There was a problem ignoring"
            })
      return response

# This function is used to do the inference on the data.
# It will connect to the triton server and send the data to it.
# The result will be returned.
# The data should be a numpy array of shape (1,10,120,120) and type float32.
# The result will be a json with the following fields:
# model_name : The name of the model used.
# outputs : The result of the inference.
def doInference(data):
      inputs = []
      outputs = []
      inputs.append(httpclient.InferInput('input_sentinel2_10_bands_120', [1, 10,120,120], "FP32"))
      inputs[0].set_data_from_numpy(data, binary_data=True)
      outputs.append(httpclient.InferRequestedOutput('representation_2048', binary_data=False))

      triton_client = httpclient.InferenceServerClient(url="bigearthnet4-predictor-default.test-bigearthnet.svc.ecmwf-inference-server.local", verbose=False)
      results = triton_client.infer(
        'Bigearth-net-ssl-label',
        inputs,
        outputs=outputs)
      return results.get_response()