import json
import os
from typing import Callable

import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from genson import SchemaBuilder

load_dotenv('../.env')
dataRoot = f"../data/{os.getenv('DATA_FOLDER')}/"
tqdmFormat = "{l_bar}{bar}| {n_fmt}/{total_fmt} [time elapsed: {elapsed}]"


def saveToFile(devices: list | dict, filePath: str, overwrite: bool = True):
    path = Path(f"{dataRoot}{filePath}")

    if not overwrite and path.exists():
        return

    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w+", encoding='utf8') as file:
        json.dump(devices, file, indent=2, ensure_ascii=False)


def getPath(filePath):
    filePath = filePath if filePath and not filePath.startswith("/") else filePath[1:]
    return Path(f"{dataRoot}{filePath}")


def saveToCsv(data: list, filePath: str, overwrite: bool = True):
    path = Path(f"{dataRoot}{filePath}")

    if not overwrite and path.exists():
        return

    path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(data)
    df.to_csv(path, index=False, encoding='utf-8-sig')


def readFile(filePath: str):
    if not pathExists(filePath):
        print('Path does not exist:', filePath)
        return None

    path = Path(f"{dataRoot}{filePath}")
    with path.open("r", encoding='utf8') as json_file:
        return json.load(json_file)


def ensureFileAndRead(filePath: str, createFile: Callable):
    if not pathExists(filePath):
        createFile()
    return readFile(filePath)


def pathExists(filePath: str):
    path = Path(f"{dataRoot}{filePath}")
    return path.exists()


def ensureTrailingSlash(func):
    def wrapper(filePath, *args, **kwargs):
        filePath = filePath if filePath and filePath.endswith("/") else filePath + "/"
        return func(filePath, *args, **kwargs)

    return wrapper


@ensureTrailingSlash
def listFileNames(folderPath: str = ''):
    path = Path(f"{dataRoot}{folderPath}")
    if not path.exists():
        print('Path does not exist: ', path)
        return []
    entries = os.listdir(path)
    files = [folderPath + entry for entry in entries if os.path.isfile(os.path.join(path, entry))]
    return files


@ensureTrailingSlash
def listDirectories(folderPath: str = ''):
    path = Path(f"{dataRoot}{folderPath}")
    if not path.exists():
        print('Path does not exist: ', path)
        return []
    entries = os.listdir(path)
    directories = [folderPath + entry for entry in entries if os.path.isdir(os.path.join(path, entry))]
    return directories


def mapToJsonSchema(event):
    alwaysPresentKeys = ["lastUpdated", "creationTime", "self", "source", "time", "id", "text", "type"]
    builder = SchemaBuilder(schema_uri=False)
    if isinstance(event, list):
        for item in event:
            builder.add_object({key: value for key, value in item.items() if key not in alwaysPresentKeys})
    else:
        builder.add_object({key: value for key, value in event.items() if key not in alwaysPresentKeys})
    jsonSchema = builder.to_schema()
    return jsonSchema
