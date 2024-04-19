import os
import json

tqdmFormat = "{l_bar}{bar}| {n_fmt}/{total_fmt} [time elapsed: {elapsed}]"


def saveToFile(devices, fileName, dataFolderName=""):
    if dataFolderName and not dataFolderName.endswith("/"): dataFolderName += "/"
    folderPath = f"../data/{dataFolderName}"
    if not os.path.exists(folderPath):
        os.makedirs(folderPath)
    with open(folderPath + fileName, 'w+', encoding='utf8') as file:
        json.dump(devices, file, indent=2, ensure_ascii=False)
