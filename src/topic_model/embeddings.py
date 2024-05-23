import os
from openai import OpenAI
from tqdm import tqdm
from skllm.config import SKLLMConfig
from src.utils import readFile, pathExists, tqdmFormat, saveToFile

SKLLMConfig.set_openai_key(os.getenv('OPENAI_API_KEY'))
SKLLMConfig.set_openai_org(os.getenv('OPENAPI_ORGANIZATION_ID'))


def requestEmbeddings(text, model="text-embedding-3-large"):
    client = OpenAI()
    return client.embeddings.create(input=text, model=model).data[0].embedding


def getDeviceEmbeddings(devices, model):
    fileName = f'topic model/{model} embeddings.json'
    deviceEmbeddings = readFile(fileName) if pathExists(fileName) else {}

    embeddings = []
    addedValues = {}
    for deviceId, device in tqdm(devices.items(), desc="requesting description embeddings", bar_format=tqdmFormat):
        if deviceId in deviceEmbeddings:
            embeddings.append(deviceEmbeddings[deviceId])
        else:
            textEmbedding = requestEmbeddings(device)
            embeddings.append(textEmbedding)
            addedValues[deviceId] = textEmbedding

    if len(addedValues) > 0:
        deviceEmbeddings.update(addedValues)
        saveToFile(deviceEmbeddings, fileName)
    return embeddings
