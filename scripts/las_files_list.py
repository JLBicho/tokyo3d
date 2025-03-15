import os
from pathlib import Path

# Define paths
ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PATH_TO_LAS_FOLDER = Path(
    os.path.join(ROOT_PATH, "las"))
file_list = os.listdir(PATH_TO_LAS_FOLDER)

with open("las_files.txt", "a") as file:
    for file_name in file_list:
        file.write(file_name + "\n")
