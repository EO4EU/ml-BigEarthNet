import rasterio
from cloudpathlib import CloudPath
import numpy as np
import cv2

import aioboto3
import aiofiles
import asyncio
from botocore.exceptions import NoCredentialsError
import concurrent.futures
import threading
import os
import logging

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


def read_data_local(parent,name,bNeedToResize):
    bands_data = []
    try:
        ALL_BANDS = BANDS_10M + BANDS_20M
        for band_name in ALL_BANDS:
            band_path = f"{parent}/{name}/{name}_{band_name}.tif"
            #logging.info('band_path '+band_path)
            with rasterio.open(band_path,driver="GTiff",sharing=False) as band_file:
                band_data   = band_file.read(1)  # open the tif image as a numpy array
                # Resize depending on the resolution
                if band_name in BANDS_20M and bNeedToResize:
                    # Carry out a bicubic interpolation (TUB does exactly this)
                    band_data = cv2.resize(band_data, dsize=(120, 120), interpolation=cv2.INTER_CUBIC)
                    # We have already ignored the 60M ones, and we keep the 10M ones intact
                #logging.info("appending")
                bands_data.append(band_data)
            band_file.close()

        bands_data = np.stack(bands_data)
        bands_data = normalize_bands(bands_data)
    except Exception as e:
        logging.info('exception in read_data_local '+str(e))
        pass
    return bands_data



async def async_read_data_local(app,parent,name, bNeedToResize,executor) -> np.ndarray:
    try:
        ALL_BANDS = BANDS_10M + BANDS_20M
        data = []
        bands_data = []
        tasks = []
        loop = asyncio.get_event_loop()
        async def fetch_band_data(band_name):
            band_path = f"{parent}/{name}/{name}_{band_name}.tif"
            try:
                async with aiofiles.open(band_path, mode='rb') as fileBand:
                    data = await fileBand.read()
                    with rasterio.io.MemoryFile(data) as memfile:
                        #app.logger.warning('memory '+threading.current_thread().name)
                        #band_data = await loop.run_in_executor(executor, read_band_data, memfile, bNeedToResize, band_name,app)
                        band_data = None
                        return band_data
            except NoCredentialsError:
                app.logger.warning("No AWS credentials found")
                return None
            except Exception as e:
                app.logger.warning('exception'+str(e))
        async with asyncio.TaskGroup() as tg:
            try:
                for band_name in ALL_BANDS:
                    tasks.append(tg.create_task(fetch_band_data(band_name)))
            except Exception as e:
                app.logger.warning('exception'+str(e))
        for task in tasks:
            band_data = await task
            bands_data.append(band_data)

        #bands_data = np.stack(bands_data)
        #bands_data = normalize_bands(bands_data)
        bands_data = None
    except Exception as e:
        app.logger.warning('exception'+str(e))
    return bands_data

def read_band_data(memfile, bNeedToResize, band_name,app):
    with memfile.open(driver="GTiff", sharing=False) as band_file:
        band_data = band_file.read(1)
        if band_name in BANDS_20M and bNeedToResize:
            band_data = cv2.resize(band_data, dsize=(120, 120), interpolation=cv2.INTER_CUBIC)
        return band_data


async def async_read_data(app,bucket_name, name, bNeedToResize,client) -> np.ndarray:
    ALL_BANDS = BANDS_10M + BANDS_20M
    data = []
    bands_data = []
    tasks = []
    async def fetch_band_data(band_name):
        band_path = f"{name}{name[:-1]}_{band_name}.tif"
        try:
            response = await client.get_object(Bucket=bucket_name, Key=band_path)
            fileBand = await response['Body'].read()
            with rasterio.io.MemoryFile(fileBand) as memfile:
                with memfile.open(driver="GTiff", sharing=False) as band_file:
                    band_data = band_file.read(1)  # open the tif image as a numpy array
                    # Resize depending on the resolution
                    if band_name in BANDS_20M and bNeedToResize:
                        # Carry out a bicubic interpolation (TUB does exactly this)
                        band_data = cv2.resize(band_data, dsize=(120, 120), interpolation=cv2.INTER_CUBIC)
                    # We have already ignored the 60M ones, and we keep the 10M ones intact

                    return band_data
        except NoCredentialsError:
            app.logger.warning("No AWS credentials found")
            return None
    async with asyncio.TaskGroup() as tg:
        try:
            for band_name in ALL_BANDS:
                tasks.append(tg.create_task(fetch_band_data(band_name)))
        except Exception as e:
            app.logger.warning('exception'+str(e))
    for task in tasks:
        band_data = await task
        bands_data.append(band_data)

    bands_data = np.stack(bands_data)
    bands_data = normalize_bands(bands_data)
    return bands_data

def read_data_s3(s3_path,keyString,NeedResize):
    try:
        cp = CloudPath("s3://"+s3_path+'/'+keyString)
        ALL_BANDS = BANDS_10M + BANDS_20M
        data = []
        name = cp.name
        bands_data = []
        for band_name in ALL_BANDS:
            band_path = cp.joinpath(f"{name}_{band_name}.tif")
            with band_path.open('rb') as fileBand, rasterio.io.MemoryFile(fileBand) as memfile:
                with memfile.open(driver="GTiff",sharing=False) as band_file:
                    band_data = band_file.read(1)  # open the tif image as a numpy array
                    # Resize depending on the resolution
                    if band_name in BANDS_20M and NeedResize:
                        # Carry out a bicubic interpolation (TUB does exactly this)
                        band_data = cv2.resize(band_data, dsize=(120, 120), interpolation=cv2.INTER_CUBIC)
                    # We have already ignored the 60M ones, and we keep the 10M ones intact

                    bands_data.append(band_data)
                band_file.close()

        bands_data = np.stack(bands_data)
        bands_data = normalize_bands(bands_data)
    except Exception as e:
        logging.info('exception in read_data_local '+str(e))
        pass
    return bands_data


def read_data(CloudPath,folder,bNeedToResize) -> np.ndarray:
    ALL_BANDS = BANDS_10M + BANDS_20M
    data = []
    name = folder
    bands_data = []
    metadata = {}
    for band_name in ALL_BANDS:
        band_path = CloudPath.joinpath(f"{name}_{band_name}.tif")
        with band_path.open('rb') as fileBand, rasterio.io.MemoryFile(fileBand) as memfile:
            with memfile.open(driver="GTiff",sharing=False) as band_file:
                metadata[band_name] = band_file.meta
                band_data = band_file.read(1)  # open the tif image as a numpy array
                # Resize depending on the resolution
                if band_name in BANDS_20M and bNeedToResize:
                    # Carry out a bicubic interpolation (TUB does exactly this)
                    band_data = cv2.resize(band_data, dsize=(120, 120), interpolation=cv2.INTER_CUBIC)
                # We have already ignored the 60M ones, and we keep the 10M ones intact

                bands_data.append(band_data)
            band_file.close()

    bands_data = np.stack(bands_data)
    bands_data = normalize_bands(bands_data)
    return bands_data,metadata

def read_data_tar(tar,path,bNeedToResize) -> np.ndarray:
    ALL_BANDS = BANDS_10M + BANDS_20M
    bands_data = []
    metadata = {}
    for band_name in ALL_BANDS:
        band_path = tar.extractfile(f"{path}_{band_name}.tif")
        with rasterio.io.MemoryFile(band_path) as memfile:
            with memfile.open(driver="GTiff",sharing=False) as band_file:
                metadata[band_name] = band_file.meta
                band_data = band_file.read(1)  # open the tif image as a numpy array
                # Resize depending on the resolution
                if band_name in BANDS_20M and bNeedToResize:
                    # Carry out a bicubic interpolation (TUB does exactly this)
                    band_data = cv2.resize(band_data, dsize=(120, 120), interpolation=cv2.INTER_CUBIC)
                # We have already ignored the 60M ones, and we keep the 10M ones intact

                bands_data.append(band_data)
            band_file.close()

    bands_data = np.stack(bands_data)
    bands_data = normalize_bands(bands_data)
    return bands_data,metadata
        
        
def normalize_bands(
        bands_data: np.ndarray,
        desired_max: int = 1,
        band_min: int = 0,
        band_max: int = 10000,
        normalize_per_band: bool = True,
    ) -> np.ndarray:
        bands_data = bands_data.astype(float)
        if normalize_per_band:
            for band_idx in range(bands_data.shape[0]):
                np.clip(bands_data[band_idx], band_min, band_max)
                bands_data[band_idx] = (
                    bands_data[band_idx] / bands_data[band_idx].max()
                ) * desired_max

        else:
            np.clip(bands_data, band_min, band_max)
            bands_data = (bands_data / bands_data.max()) * desired_max
        return bands_data
