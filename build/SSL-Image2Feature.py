from flask import Flask, request, make_response
import uuid
import json
import sys
from cloudpathlib import CloudPath
from cloudpathlib import S3Client
import BigEarthNetLoader
import tritonclient.http.aio as httpclient
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

from pathlib import Path

import threading

import rasterio

import cv2

import zipfile

from KafkaHandler import KafkaHandler,DefaultContextFilter

import logging

import asyncio

import time

def create_app():

      app = Flask(__name__)

      Producer=KafkaProducer(bootstrap_servers="kafka-external.dev.apps.eo4eu.eu:9092",value_serializer=lambda v: json.dumps(v).encode('utf-8'),key_serializer=str.encode)
      handler = KafkaHandler(defaultproducer=Producer)
      console_handler = logging.StreamHandler()
      console_handler.setLevel(logging.DEBUG)
      filter = DefaultContextFilter()
      app.logger.addFilter(filter)
      app.logger.addHandler(handler)
      app.logger.addHandler(console_handler)
      app.logger.setLevel(logging.DEBUG)

      logger_app = logging.LoggerAdapter(app.logger, {'source': 'ML.uc6.classifier'},merge_extra=True)
      logger_app.info("Application Starting up...", extra={'status': 'DEBUG'})

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
                  api_response = api_instance.read_namespaced_config_map(configmap_name, configmap_namespace)
                  json_data_request = json.loads(request.data)
                  json_data_configmap =json.loads(str(api_response.data['jsonSuperviserRequest']))      
                  workflow_name = json_data_configmap.get('workflow_name', '')
                  bootstrapServers =api_response.data['bootstrapServers']
                  Producer=KafkaProducer(bootstrap_servers=bootstrapServers,value_serializer=lambda v: json.dumps(v).encode('utf-8'),key_serializer=str.encode)
                  logger_workflow = logging.LoggerAdapter(logger_app, {'workflow_name': workflow_name,'producer':Producer},merge_extra=True)
                  logger_workflow.info('Starting Workflow',extra={'status':'START'})
                  logger_workflow.info('Reading json data request'+str(json_data_request), extra={'status': 'DEBUG'})
                  logger_workflow.info('Reading json data configmap'+str(json_data_configmap), extra={'status': 'DEBUG'})

                  if not(json_data_request['previous_component_end'] == 'True' or json_data_request['previous_component_end']):
                        class PreviousComponentEndException(Exception):
                              pass
                        raise PreviousComponentEndException('Previous component did not end correctly')

                  kafka_out = json_data_configmap['Topics']["out"]
                  s3_access_key = json_data_configmap['S3_bucket']['aws_access_key_id']
                  s3_secret_key = json_data_configmap['S3_bucket']['aws_secret_access_key']
                  s3_bucket_output = json_data_configmap['S3_bucket']['s3-bucket-name']
                  s3_region_endpoint = json_data_configmap['S3_bucket']['endpoint_url']
                  s3_path = json_data_request['S3_bucket_desc']['folder']
                  s3_file = json_data_request['S3_bucket_desc'].get('filename',None)

                  def threadentry():
                        try:
                              logger_workflow.info('All json data read', extra={'status': 'INFO'})
                              clientS3 = S3Client(aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key,endpoint_url=s3_region_endpoint)
                              clientS3.set_as_default_client()
                              logger_workflow.info('Client is ready', extra={'status': 'INFO'})
                              cp = CloudPath("s3://"+s3_bucket_output+'/'+s3_path+'/INSITU', client=clientS3)
                              cpOutput = CloudPath("s3://"+s3_bucket_output+'/result-image2feature/')
                              logger_workflow.info("path is s3://"+s3_bucket_output+'/result-image2feature/', extra={'status': 'DEBUG'})
                              def fatalError(message):
                                    logger_workflow.error(message, extra={'status': 'CRITICAL'})
                              
                              with cpOutput.joinpath('log.txt').open('w') as fileOutput:
                                    meta=None
                                    def treatFolder(folder):
                                                pattern=r'.*MSIL2A.*\.SAFE$'
                                                match = re.search(pattern,folder.name)
                                                if match:
                                                      logger_workflow.info('matched folder '+str(folder), extra={'status': 'DEBUG'})
                                                      with tempfile.TemporaryDirectory() as tempdirBen:
                                                            cpGranule=folder/"GRANULE"
                                                            dicPath={}
                                                            for folderGranule in cpGranule.iterdir():
                                                                  logger_workflow.info("granule path "+str(folderGranule),extra={'status': 'DEBUG'})
                                                                  cpIMGData10m=folderGranule/"IMG_DATA"/"R10m"
                                                                  for image in cpIMGData10m.iterdir():
                                                                        pattern=r'.*_(.*)_10m\.jp2$'
                                                                        match = re.search(pattern,image.name)
                                                                        logger_workflow.info("image path "+str(image),extra={'status': 'DEBUG'})
                                                                        if match:
                                                                              matchedBand=match.group(1)
                                                                              logger_workflow.info("matchedBand "+matchedBand,extra={'status': 'DEBUG'})
                                                                              if matchedBand in ['B02','B03','B04','B08']:
                                                                                    logger_workflow.info("matchedBand 10m "+matchedBand,extra={'status': 'DEBUG'})
                                                                                    path_src=image
                                                                                    dicPath[matchedBand]=path_src
                                                                        else:
                                                                              logger_workflow.info("not matched",extra={'status': 'DEBUG'})
                                                                  cpIMGData20m=folderGranule/"IMG_DATA"/"R20m"
                                                                  for image in cpIMGData20m.iterdir():
                                                                        pattern=r'.*_(.*)_20m\.jp2$'
                                                                        match = re.search(pattern,image.name)
                                                                        logger_workflow.info("image path "+str(image),extra={'status': 'DEBUG'})
                                                                        if match:
                                                                              logger_workflow.info("matched",extra={'status': 'DEBUG'})
                                                                              matchedBand=match.group(1)
                                                                              logger_workflow.info("matchedBand "+matchedBand,extra={'status': 'DEBUG'})
                                                                              if matchedBand in ['B05','B06','B07','B8A','B11','B12']:
                                                                                    logger_workflow.info("matchedBand 20m "+matchedBand,extra={'status': 'DEBUG'})
                                                                                    path_src=image
                                                                                    dicPath[matchedBand]=path_src
                                                                        else:
                                                                              logger_workflow.info("not matched",extra={'status': 'DEBUG'})
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
                                                                        if band_name not in dicPath:
                                                                              logger_workflow.info("band_name "+band_name+" not found. Stopping treating folder "+str(folder),extra={'status': 'INFO'})
                                                                              return
                                                                        band_path = dicPath[band_name]
                                                                        logger_workflow.info("band_path "+str(band_path),extra={'status': 'DEBUG'})
                                                                        with band_path.open('rb') as fileBand, rasterio.io.MemoryFile(fileBand) as memfile:
                                                                              with memfile.open(sharing=False) as band_file:
                                                                                    band_data   = band_file.read(1,masked=True)  # open the tif image as a numpy array
                                                                                    band_data=band_data.filled(np.nan)
                                                                                    # Resize depending on the resolution
                                                                                    if band_name in BANDS_20M:
                                                                                          h=band_data.shape[0]
                                                                                          w=band_data.shape[1]
                                                                                          # Carry out a bicubic interpolation (TUB does exactly this)
                                                                                          band_data = cv2.resize(band_data, dsize=(2*h, 2*w), interpolation=cv2.INTER_CUBIC)
                                                                                          # We have already ignored the 60M ones, and we keep the 10M ones intact
                                                                                    #logging.info("appending")
                                                                                    bands_data.append(band_data)
                                                                                    logger_workflow.info("band_name "+band_name,extra={'status': 'DEBUG'})
                                                                                    logger_workflow.info("band_data shape "+str(band_data.shape),extra={'status': 'DEBUG'})
                                                                        band_file.close()

                                                                  bands_data = np.stack(bands_data)
                                                                  shape = bands_data.shape
                                                                  h=shape[1]
                                                                  w=shape[2]
                                                                  if h<120 or w<120:
                                                                        logger_workflow.info("Dimension too small, it should be at least 120. h "+str(h)+" w "+str(w)+"Stopping treating folder "+str(folder),extra={'status': 'INFO'})
                                                                  to_infer=[]
                                                                  for i in range(0,h,120):
                                                                        for j in range(0,w,120):
                                                                              view_data=bands_data[:,i:i+120,j:j+120]
                                                                              valid=False
                                                                              if view_data.shape[1]==120 and view_data.shape[2]==120:
                                                                                    valid=True
                                                                              if np.isnan(view_data).any():
                                                                                    valid=False
                                                                              if valid:
                                                                                    dic={}
                                                                                    dic['i']=i
                                                                                    dic['j']=j
                                                                                    dic['data']=bands_data
                                                                                    to_infer.append(dic)
                                                            asyncio.run(doInference(to_infer,logger_workflow))
                                                            for elem in to_infer:
                                                                  elem['data']=None
                                                            with cpOutput.joinpath(folder.name+'.json').open('w') as outputFile:
                                                                  json.dump(to_infer, outputFile)
                                                            #bands_data = BigEarthNetLoader.normalize_bands(bands_data)
                                                            # data=np.expand_dims(bands_data.astype(np.float32),axis=0)
                                                            # result=doInference(data)
                                                            # outputPath=cpOutput.joinpath(folderOutput.name)
                                                            # with outputPath.open('w') as outputFile:
                                                            #       json.dump(result, outputFile) 
                                    for folder in cp.iterdir():
                                          treatFolder(folder)
                              logger_workflow.info('Connecting to Kafka', extra={'status': 'DEBUG'})

                              response_json ={
                              "previous_component_end": "True",
                              "S3_bucket_desc": {
                                    "folder": "result-image2feature","filename": ""
                              },
                              "meta_information": json_data_request.get('meta_information',{})}
                              Producer.send(kafka_out,key='key',value=response_json)
                              Producer.flush()                
                                                
                                                
                        except Exception as e:
                              logger_workflow.error('Got exception '+str(e)+'\n'+traceback.format_exc()+'\n'+'So we are ignoring the message', extra={'status': 'CRITICAL'})
                              return
                        logger_workflow.info('workflow finished successfully',extra={'status':'SUCCESS'})

                  thread = threading.Thread(target=threadentry)
                  thread.start()
                  response = make_response({
                              "msg": "Started the process"
                              })
            except Exception as e:
                  logger_workflow.error('Got exception '+str(e)+'\n'+traceback.format_exc()+'\n'+'So we are ignoring the message', extra={'status': 'CRITICAL'})
                  # HTTP answer that the message is malformed. This message will then be discarded only the fact that a sucess return code is returned is important.
                  response = make_response({
                  "msg": "There was a problem ignoring"
                  })
            return response
      
      async def doInference(toInfer,logger_workflow):

            triton_client = httpclient.InferenceServerClient(url="default-inference.shared.svc.cineca-inference-server.local", verbose=False,conn_timeout=10000000000,conn_limit=None,ssl=False)
            nb_Created=0
            nb_InferenceDone=0
            nb_Postprocess=0
            nb_done_instance=0
            list_postprocess=set()
            list_task=set()
            last_throw=0
            lookup={}

            async def consume(task):
                  try:
                        if task[0]==1:
                              count=task[1]
                              inputs=[]
                              outputs=[]
                              iCord=toInfer[count]["i"]
                              jCord=toInfer[count]["j"]
                              logger_workflow.info('orig shape '+str(toInfer[count]["data"].shape), extra={'status': 'DEBUG'})
                              logger_workflow.info('iCord '+str(iCord)+' jCord '+str(jCord), extra={'status': 'DEBUG'})
                              data=toInfer[count]["data"][:,iCord:iCord+120,jCord:jCord+120]
                              data=BigEarthNetLoader.normalize_bands(data)
                              logger_workflow.info('data shape '+str(data.shape), extra={'status': 'DEBUG'})
                              #BigEarthNetLoader.normalize_bands(data)
                              data=np.expand_dims(data.astype(np.float32),axis=0)
                              inputs.append(httpclient.InferInput('input_sentinel2_10_bands_120',data.shape, "FP32"))
                              inputs[0].set_data_from_numpy(data, binary_data=True)
                              del data
                              outputs.append(httpclient.InferRequestedOutput('representation_2048', binary_data=True))
                              results = await triton_client.infer('Bigearth-net-ssl-label',inputs,outputs=outputs)
                              return (task,results)
                                    #toInfer[count]["result"]=results.as_numpy('probability')[0][0]
                  except Exception as e:
                        logger_workflow.error('Got exception in inference '+str(e)+'\n'+traceback.format_exc(), extra={'status': 'WARNING'})
                        nonlocal last_throw
                        last_throw=time.time()
                        return await consume(task)
                  
            async def postprocess(task,results):
                  if task[0]==1:
                        result=results.as_numpy('representation_2048')[0]
                        toInfer[task[1]]["result"]=result.tolist()

            def postprocessTask(task):
                  list_task.discard(task)
                  new_task=asyncio.create_task(postprocess(*task.result()))
                  list_postprocess.add(new_task)
                  def postprocessTaskDone(task2):
                        nonlocal nb_Postprocess
                        nb_Postprocess+=1
                        nonlocal nb_done_instance
                        nb_done_instance+=task.result()[0][0]
                        list_postprocess.discard(task2)
                  new_task.add_done_callback(postprocessTaskDone)
                  nonlocal nb_InferenceDone
                  nb_InferenceDone+=1

            def producer():
                  total=len(toInfer)
                  count=0
                  while total-count>=1:
                        yield (1,count)
                        count=count+1

            last_shown=time.time()
            start=time.time()-60
            for item in producer():
                  while time.time()-last_throw<30 or nb_Created-nb_InferenceDone>5 or nb_Postprocess-nb_InferenceDone>5:
                        await asyncio.sleep(0)
                  task=asyncio.create_task(consume(item))
                  list_task.add(task)
                  task.add_done_callback(postprocessTask)
                  nb_Created+=1
                  if time.time()-last_shown>60:
                        last_shown=time.time()
                        logger_workflow.info('done instance '+str(nb_done_instance)+'Inference done value '+str(nb_InferenceDone)+' postprocess done '+str(nb_Postprocess)+ ' created '+str(nb_Created), extra={'status': 'DEBUG'})
            while nb_InferenceDone-nb_Created>0 or nb_Postprocess-nb_InferenceDone>0:
                  await asyncio.sleep(0)
            await asyncio.gather(*list_task,*list_postprocess)
            logger_workflow.info('Inference done', extra={'status': 'DEBUG'})
            await triton_client.close()
      return app

                        

# # This function is used to do the inference on the data.
# # It will connect to the triton server and send the data to it.
# # The result will be returned.
# # The data should be a numpy array of shape (1,10,120,120) and type float32.
# # The result will be a json with the following fields:
# # model_name : The name of the model used.
# # outputs : The result of the inference.
# def doInference(data):
#       inputs = []
#       outputs = []
#       inputs.append(httpclient.InferInput('input_sentinel2_10_bands_120', [1, 10,120,120], "FP32"))
#       inputs[0].set_data_from_numpy(data, binary_data=True)
#       outputs.append(httpclient.InferRequestedOutput('representation_2048', binary_data=False))

#       triton_client = httpclient.InferenceServerClient(url="bigearthnet4-predictor-default.test-bigearthnet.svc.ecmwf-inference-server.local", verbose=False)
#       results = triton_client.infer(
#         'Bigearth-net-ssl-label',
#         inputs,
#         outputs=outputs)
#       return results.get_response()
      return app