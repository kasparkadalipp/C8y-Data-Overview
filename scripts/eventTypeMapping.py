import os
from collections import Counter
from src.utils import fileNamesInFolder, readFile
from src.utils import readFileContents, saveToFile
import json
from tabulate import tabulate


def listDirectories(path):
    entries = os.listdir(path)
    directories = [entry for entry in entries if os.path.isdir(os.path.join(path, entry))]
    return directories


def createEventTypeMapping():
    c8y_data = readFile('telia/c8y_data.json')
    eventTypesMapping = {device['id']: set() for device in c8y_data}

    for folder in listDirectories('../data/telia/events'):
        for file in fileNamesInFolder(f'../data/telia/events/{folder}'):
            jsonFile = readFileContents(f'../data/telia/events/{folder}/{file}')
            for device in jsonFile:
                deviceId = device['deviceId']
                if 'total' in device:
                    event = device['total']['event']
                    if event:
                        eventTypesMapping[deviceId].add(event['type'])

                if 'eventByType' in device:
                    for eventByType in device['eventByType']:
                        event = eventByType['event']
                        if event:
                            eventTypesMapping[deviceId].add(event['type'])

    data = {key: sorted(value) for key, value in eventTypesMapping.items()}
    saveToFile(data, "telia/c8y_events_id_to_type_mapping.json", overwrite=True)


def mappingOverview():
    with open("../data/telia/c8y_events_id_to_type_mapping.json", "r", encoding='utf8') as json_file:
        jsonData = json.load(json_file)
    counter = Counter([tuple(value) for key, value in jsonData.items() if value])

    data = [(value, key) for key, value in counter.items()]
    table = tabulate(data, headers=["Count", "Types"], tablefmt="pipe")
    print(table)
