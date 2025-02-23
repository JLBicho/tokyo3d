import struct
import base64
from pathlib import Path
import os
import json
from multiprocessing import Pool
import time
import argparse

import zipfile
import urllib
import numpy as np
import laspy

from mcap.well_known import SchemaEncoding, MessageEncoding
from mcap.writer import Writer

# Define paths
ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
URLLIST_PATH = os.path.join(ROOT_PATH, "urllist.txt")
PATH_TO_SCHEMA_FOLDER = Path(
    os.path.join(ROOT_PATH, "schemas"))
PATH_TO_LAS_FOLDER = Path(
    os.path.join(ROOT_PATH, "las"))
PATH_TO_ZIP_FOLDER = Path(
    os.path.join(ROOT_PATH, "zip"))
PATH_TO_OUTPUT_FOLDER = Path(
    os.path.join(ROOT_PATH, "output"))

# Create output folders
if not os.path.exists(PATH_TO_LAS_FOLDER):
    os.mkdir(PATH_TO_LAS_FOLDER)
if not os.path.exists(PATH_TO_ZIP_FOLDER):
    os.mkdir(PATH_TO_ZIP_FOLDER)
if not os.path.exists(PATH_TO_OUTPUT_FOLDER):
    os.mkdir(PATH_TO_OUTPUT_FOLDER)


def download(url: str, fname: str):
    start_t = time.time()
    print(f"Downloading {url} to {fname}")
    urllib.request.urlretrieve(url, fname)
    print(
        f"DONE downloading file {fname} in {round(time.time()-start_t)} seconds")
    mid_t = time.time()
    with zipfile.ZipFile(fname) as zipf:
        zipf.extractall(PATH_TO_LAS_FOLDER)
    print(f"DONE unzipping file {fname} in {round(time.time()-mid_t)} seconds")


def download_files():
    print("Downloading files")
    # Download files
    with open(URLLIST_PATH, "r", encoding="utf-8") as f:
        urls = f.readlines()
        urls = [url.replace("\n", "") for url in urls]

    print(f"Found {len(urls)} files to download.")

    with Pool(10) as pool:
        pool.starmap(download, [(url, (os.path.join(PATH_TO_ZIP_FOLDER, url.split("/")[-1]).replace("\n", "")))
                                for url in urls])


def generate_channel_id(channels: dict, writer: Writer, json_name: str, topic: str):
    """ Generate a topic channel_id for the specified message type """
    with open(os.path.join(PATH_TO_SCHEMA_FOLDER, json_name+".json"), "rb") as f:
        schema = f.read()
        pressure_schema_id = writer.register_schema(
            name="foxglove."+json_name,
            encoding=SchemaEncoding.JSONSchema,
            data=schema)
        pressure_channel_id = writer.register_channel(
            topic=topic,
            message_encoding=MessageEncoding.JSON,
            schema_id=pressure_schema_id)
        channels[topic] = pressure_channel_id


def getXYZRGB(point) -> list:
    x = point[0]*0.001
    y = point[1]*0.001
    z = point[2]*0.001
    r = int(point[-3]/65535*255)
    g = int(point[-2]/65535*255)
    b = int(point[-1]/65535*255)
    a = 255
    return [x, y, z, r, g, b, a]


def generate_mcap(mcap_filename: str, max_points: int):
    channel_topic = ("PointCloud", "point_cloud")
    timestamp = {"sec": 0, "nsec": 0}

    pointcloud = {
        "position": {"x": 0, "y": 0, "z": 0},
        "orientation": {"x": 0, "y": 0, "z": 0, "w": 1},
        "frame_id": "tokyo3d",
        "point_stride": (4 + 4 + 4 + 4),
        "fields": [
            {"name": "x", "offset": 0, "type": 7},
            {"name": "y", "offset": 4, "type": 7},
            {"name": "z", "offset": 8, "type": 7},
            {"name": "alpha", "offset": 12, "type": 1},
            {"name": "red", "offset": 13, "type": 1},
            {"name": "green", "offset": 14, "type": 1},
            {"name": "blue", "offset": 15, "type": 1},
        ]
    }

    las_files = os.listdir(PATH_TO_LAS_FOLDER)
    print(f"Found {len(las_files)} '.las' files.")
    SUBSAMPLE = round(max_points/len(las_files))

    with open(os.path.join(PATH_TO_OUTPUT_FOLDER, mcap_filename), "wb") as f:
        writer = Writer(f)
        writer.start("x-jsonschema")
        channels = {}

        generate_channel_id(
            channels, writer, channel_topic[0], channel_topic[1])

        points = bytearray()
        point_struct = struct.Struct("<fffBBBB")
        total_points = 0
        for i_las, las_file in enumerate(las_files):
            with laspy.open(os.path.join(PATH_TO_LAS_FOLDER, las_file)) as fh:
                print(
                    f'File: {las_file} ({i_las+1}/{len(las_files)}) with {fh.header.point_count} points.')
                if fh.header.point_count == 0:
                    print("Skipping empty file")
                    continue

                las = fh.read()

                last_print = 0
                points_array = las.points.array.flatten()
                max_subsample = min(SUBSAMPLE, len(points_array))
                random_points = np.random.choice(
                    points_array, max_subsample, replace=False)
                print(
                    f"Orignal array size: {len(points_array)}. New array size: {len(random_points)}.")
                for i, point in enumerate(random_points):
                    x, y, z, r, g, b, a = getXYZRGB(point)
                    points.extend(point_struct.pack(x, y, z, a, r, g, b))

                    current_percentage = i/len(random_points)*100
                    if current_percentage - last_print > 5:
                        print(f"{round(current_percentage)}%")
                        last_print = current_percentage

                total_points += len(random_points)
                print(f"Total points: {total_points}")

        pointcloud["data"] = base64.b64encode(
            points).decode('utf-8')

        pointcloud["timestamp"] = {
            "sec": 0, "nsec": int(timestamp["nsec"])}

        writer.add_message(
            channels["point_cloud"],
            log_time=int(pointcloud["timestamp"]["nsec"]),
            data=json.dumps(pointcloud).encode("utf-8"),
            publish_time=int(pointcloud["timestamp"]["nsec"]),
        )
        points.clear()
        print("Finished")

        writer.finish()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--download", action="store_true",
                        default=False)
    parser.add_argument("--mcap_filename",
                        action="store_true", default="tokyo.mcap")
    parser.add_argument("--points", action="store_true", default=10000000)

    args = parser.parse_args()
    DOWNLOAD = args.download
    MCAP_FILENAME = args.mcap_filename
    MAX_POINTS = args.points

    if DOWNLOAD:
        download_files()
    generate_mcap(mcap_filename=MCAP_FILENAME, max_points=MAX_POINTS)
