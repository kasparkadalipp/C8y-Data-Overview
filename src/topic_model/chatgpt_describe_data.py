import os
from collections import defaultdict
import instructor
import pandas as pd
from openai import OpenAI
from pydantic import BaseModel
from skllm.config import SKLLMConfig
from src.utils import tqdmFormat, getPath, readFile, saveToFile, listFileNames, ensureFileAndRead
from tqdm import tqdm


class Measurement(BaseModel):
    description: str
    domain: str
    subdomain: str


def requestChatGPTDescription(client, message_data, model):
    return client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content":
                    'Take on the persona of a data analyst who is proficient in interpreting JSON objects and '
                    'extracting meaningful insights from them. '
                    'The user will provide JSON objects representing measurement data from a smart city IoT device.'

            },
            {
                "role": "user",
                "content": 'Ignore device specific information and concisely summarise what kind of data is being sent. '
                           'Avoid generic terms like "IoT" and "smart city". '
                           'Also provide example of a smart city domain this device belongs to.'
                           f"{message_data} "
            }
        ],
        max_retries=10,
        response_model=Measurement
    )


def describeDeviceData(model="gpt-4-turbo"):
    inputData = ensureFileAndRead('topic model/chatGPT input.json', createDatasetMapping)

    SKLLMConfig.set_openai_key(os.getenv('OPENAI_API_KEY'))
    SKLLMConfig.set_openai_org(os.getenv('OPENAPI_ORGANIZATION_ID'))
    client = instructor.patch(OpenAI())

    result = {}
    failedRequests = []
    for deviceId, data in tqdm(inputData.items(), desc=f"requesting device descriptions for {model}", bar_format=tqdmFormat):
        try:
            response = requestChatGPTDescription(client, data, model)
            result[deviceId] = {
                'id': deviceId,
                'name': data['device'],
                'domain': response.domain,
                'subdomain': response.subdomain,
                'description': response.description,
                'input': data
            }
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(e)
            failedRequests.append(deviceId)

    df = pd.DataFrame(result.values())
    df.to_csv(getPath(f"{model} description.csv"), encoding='utf-8-sig', index=False)

    documents = {deviceId: f"{device['domain']} {device['subdomain']} {device['description']}" for deviceId, device in result.items()}
    saveToFile(documents, f"topic model/{model} descriptions.json")
    return df


def formatUnits(units):
    units = [unit for unit in units if unit is not None and unit != '']
    if len(units) == 1:
        return units[0]
    if len(units) == 0:
        return ''
    return units


def shortenEvent(event):
    def process_node(node, path=tuple()):
        if isinstance(node, dict):
            return {k: process_node(v, path + (k,)) for k, v in node.items()}
        elif isinstance(node, list):
            # Retain only the first element of the array
            if len(node) > 1:
                return [process_node(node[0], path)] if node else []
            return [process_node(node[0], path)] if node else []
        else:
            return node

    return process_node(event)


def createMeasurementMapping():
    measurementMapping = defaultdict(lambda: {'count': 0, 'units': set(), 'example': {}})
    for fileName in listFileNames('measurements/typeFragmentSeries/'):
        for device in readFile(fileName):
            deviceType = device['deviceType']
            deviceId = device['deviceId']

            for fragmentSeries in device['typeFragmentSeries']:
                measurementType = fragmentSeries['type']
                fragment = fragmentSeries['fragment']
                series = fragmentSeries['series']
                count = fragmentSeries['count']

                measurement = fragmentSeries['measurement']
                if measurement:
                    measurementValue = measurement[fragment][series]
                    unit = measurementValue['unit'] if 'unit' in measurementValue else ''
                    key = (deviceId, deviceType, measurementType, fragment, series)
                    measurementMapping[key]['units'].add(unit)
                    measurementMapping[key]['count'] += count
                    measurementMapping[key]['example'] = measurement
    return measurementMapping


def createEventMapping():
    eventMapping = {}
    for fileName in listFileNames('events/type/'):
        for event in readFile(fileName):
            deviceId = event['deviceId']
            deviceType = event['deviceType']
            for eventTypeObj in event['eventByType']:
                eventType = eventTypeObj['type']
                event = eventTypeObj['event']
                if event:
                    key = (deviceId, deviceType, eventType)
                    if key in eventMapping:
                        continue
                    eventMapping[key] = event
    return eventMapping


def createDatasetMapping():
    c8y_data = readFile('c8y_data.json')
    deviceIdMapping = {device['id']: device for device in c8y_data}
    measurementMapping = createMeasurementMapping()
    eventMapping = createEventMapping()

    deviceMapping = {}
    for key, value in measurementMapping.items():
        deviceId, deviceType, measurementType, fragment, series = key
        deviceName = deviceIdMapping[deviceId]['name']
        units = formatUnits(value['units'])

        if deviceId not in deviceMapping:
            deviceMapping[deviceId] = {'device': deviceName}

        device = deviceMapping[deviceId]
        if fragment not in device:
            device[fragment] = {}
        if series not in device[fragment]:
            device[fragment][series] = units

    for key, event in eventMapping.items():
        deviceId, deviceType, eventType = key
        deviceName = deviceIdMapping[deviceId]['name']
        if deviceId not in deviceMapping:
            deviceMapping[deviceId] = {'device': deviceName}

        alwaysPresentKeys = ["lastUpdated", "creationTime", "self", "source", "time", "id", "text", "type"]
        exampleEvent = {key: value for key, value in event.items() if key not in alwaysPresentKeys}

        deviceMapping[deviceId][eventType] = shortenEvent(exampleEvent)

    saveToFile(deviceMapping, "topic model/chatGPT input.json")
