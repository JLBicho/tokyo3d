import struct
import base64
from pathlib import Path
import os
import json
from multiprocessing import Pool
import time
import argparse

import zipfile
import urllib.request
import numpy as np
import laspy

from mcap.well_known import SchemaEncoding, MessageEncoding
from mcap.writer import Writer

from mcap_ros2.writer import Writer as WriterRos2
from PointCloud2 import PC2_SCHEMA_NAME, PC2_SCHEMA_TEXT

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


def download_and_generate_mcap(url: str, fname: str, mcap_filename: str, max_points: int, use_ros2: bool = False, delete_las: bool = False):
    download(url, fname)
    generate_mcap(mcap_filename=mcap_filename, max_points=max_points,
                  use_ros2=use_ros2, delete_las=delete_las)


def download(url: str, fname: str):
    filename = fname.split("/")[-1].replace(".zip", ".las")
    print(f"Checking if {filename} already exists.")
    las_files_in_dir = os.listdir(PATH_TO_LAS_FOLDER)
    with open("las_files.txt", "r") as f:
        las_files_in_txt = f.readlines()
        las_files_in_txt = [file.replace("\n", "")
                            for file in las_files_in_txt]
    las_files = list(set(las_files_in_dir + las_files_in_txt))
    if filename in las_files:
        print(f"File {filename} already exists. Skipping download.")
        return
    start_t = time.time()
    try:
        print(f"Downloading {url} to {fname}")
        urllib.request.urlretrieve(url, fname)
        print(
            f"DONE downloading file {fname} in {round(time.time()-start_t)} seconds")
        mid_t = time.time()
        with zipfile.ZipFile(fname) as zipf:
            zipf.extractall(PATH_TO_LAS_FOLDER)
        print(
            f"DONE unzipping file {fname} in {round(time.time()-mid_t)} seconds")
        print(f"Removing {fname}")
        os.remove(fname)
    except Exception as e:
        print(f"Error downloading file {fname}: {e}")


def download_files():
    print("Downloading files")
    # Download files
    with open(URLLIST_PATH, "r", encoding="utf-8") as f:
        urls = f.readlines()
        urls = [url.replace("\n", "") for url in urls]

    print(f"Found {len(urls)} files to download.")

    with Pool(20) as pool:
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


def getXYZRGBA(point) -> list:
    x = point[0]*0.001
    y = point[1]*0.001
    z = point[2]*0.001
    r = int(point[-3]/65535*255)
    g = int(point[-2]/65535*255)
    b = int(point[-1]/65535*255)
    a = 255
    return [x, y, z, r, g, b, a]


def get_unpacked_XYZRGBA(point) -> list:
    x = point[0]*0.001
    y = point[1]*0.001
    z = point[2]*0.001
    r = int(point[-3]/65535*255)
    g = int(point[-2]/65535*255)
    b = int(point[-1]/65535*255)
    a = 255
    unpacked = struct.unpack(
        'BBBBBBBBBBBBBBBB', struct.pack('fffBBBB', x, y, z, b, g, r, a))
    return unpacked


def generate_mcap(mcap_filename: str, max_points: int, use_ros2: bool = False, delete_las: bool = False):
    channel_topic = ("PointCloud", "/point_cloud")
    timestamp = {"sec": 0, "nsec": 1e9}

    if use_ros2:
        pointcloud = {
            "header": {
                "frame_id": "tokyo3d",
                "stamp": {"sec": 0, "nanosec": int(timestamp["nsec"])}
            },
            "height": 1,
            "width": max_points,
            "frame_id": "tokyo3d",
            "point_step": (4 + 4 + 4 + 4),
            "row_step": (4 + 4 + 4 + 4) * max_points,
            "is_bigendian": False,
            "is_dense": True,
            "fields": [
                {"name": "x", "offset": 0, "datatype": 7, "count": 1},
                {"name": "y", "offset": 4, "datatype": 7, "count": 1},
                {"name": "z", "offset": 8, "datatype": 7, "count": 1},
                {"name": "rgba", "offset": 12, "datatype": 6, "count": 1},
            ]
        }
    else:
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

    iterations = int(len(las_files)/2)
    for it in range(int(iterations)):
        print(f"Iteration {it+1}/{iterations}")

        with open(os.path.join(PATH_TO_OUTPUT_FOLDER, mcap_filename.replace('.mcap', f'_{it}.mcap')), "wb") as f:
            las_files_chunk = las_files[it*2:it*2+2]
            if use_ros2:
                writer = WriterRos2(f)
            else:
                writer = Writer(f)
                writer.start("x-jsonschema")
            channels = {}

            if use_ros2:
                schema = writer.register_msgdef(
                    PC2_SCHEMA_NAME, PC2_SCHEMA_TEXT)
            else:
                generate_channel_id(
                    channels, writer, channel_topic[0], channel_topic[1])

            if use_ros2:
                pointcloud["data"] = []
            else:
                points = bytearray()
                point_struct = struct.Struct("<fffBBBB")
            total_points = 0
            for i_las, las_file in enumerate(las_files_chunk):
                try:
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
                        f"Original array size: {len(points_array)}. New array size: {len(random_points)}.")

                    for i, point in enumerate(random_points):
                        if use_ros2:
                            pt_xyzrgba = get_unpacked_XYZRGBA(point)
                            pointcloud["data"].extend(pt_xyzrgba)
                        else:
                            x, y, z, r, g, b, a = getXYZRGBA(point)
                            points.extend(point_struct.pack(
                                x, y, z, a, r, g, b))

                        current_percentage = i/len(random_points)*100
                        if current_percentage - last_print > 5:
                            print(f"{round(current_percentage)}%")
                            last_print = current_percentage

                    total_points += len(random_points)
                    print(f"Total points: {total_points}")

                    if use_ros2:
                        print("Writing message")
                        writer.write_message(
                            topic=channel_topic[1],
                            schema=schema,
                            message=pointcloud,
                            log_time=int(timestamp["nsec"]),
                            publish_time=int(timestamp["nsec"]),
                            sequence=0)
                        pointcloud["data"] = []

                    print("-------------------")
                except Exception as e:
                    print(f"Error processing file {las_file}: {e}")
                    continue

            if not use_ros2:
                pointcloud["data"] = base64.b64encode(
                    points).decode('utf-8')

                pointcloud["timestamp"] = {
                    "sec": 0, "nsec": int(timestamp["nsec"])}

            if not use_ros2:
                print("Writing message")
                writer.add_message(
                    channels[channel_topic[1]],
                    log_time=int(pointcloud["timestamp"]["nsec"]),
                    data=json.dumps(pointcloud).encode("utf-8"),
                    publish_time=int(pointcloud["timestamp"]["nsec"]),
                )
            # points.clear()
            print("Finished")

            writer.finish()

            if delete_las:
                for file in las_files_chunk:
                    os.remove(os.path.join(PATH_TO_LAS_FOLDER, file))
                    print(f"Deleted {file}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--download", action="store_true")
    parser.add_argument("--use-ros2", action="store_true")
    parser.add_argument("--delete-las", action="store_true")
    parser.add_argument("--mcap-filename", default="tokyo.mcap")
    parser.add_argument("--points", default=30000000, type=int)

    args = parser.parse_args()
    DOWNLOAD = args.download
    MCAP_FILENAME = args.mcap_filename
    if not MCAP_FILENAME.endswith(".mcap"):
        MCAP_FILENAME += ".mcap"
    MAX_POINTS = args.points
    USE_ROS2 = args.use_ros2
    DELETE_LAS = args.delete_las

    if DOWNLOAD:
        try:
            download_files()
        except Exception as e:
            print(f"Error downloading files: {e}")

    generate_mcap(mcap_filename=MCAP_FILENAME,
                  max_points=MAX_POINTS, use_ros2=USE_ROS2, delete_las=DELETE_LAS)
