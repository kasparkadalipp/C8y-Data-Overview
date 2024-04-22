import json
import os
from pathlib import Path

tqdmFormat = "{l_bar}{bar}| {n_fmt}/{total_fmt} [time elapsed: {elapsed}]"

dataRoot = "../data/"


def saveToFile(devices: list, filePath: str, overwrite: bool):
    path = Path(f"{dataRoot}{filePath}")

    if not overwrite and path.exists():
        return 'skipped'

    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w+", encoding='utf8') as file:
        json.dump(devices, file, indent=2, ensure_ascii=False)


def pathExists(filePath: str):
    path = Path(f"{dataRoot}{filePath}")
    return path.exists()


def filePathsInFolder(folder):
    if not pathExists(folder):
        print('Folder does not exist: ' + folder)
        return []
    folder += '' if folder.endswith("/") else '/'
    fileNames = os.listdir(folder)
    return [folder + file for file in fileNames]


def readFileContents(filePath):
    with open(filePath, 'r', encoding='utf8') as json_file:
        return json.load(json_file)


def fileContentsFromFolder(folderPath):
    return [readFileContents(filePath) for filePath in filePathsInFolder(folderPath)]
