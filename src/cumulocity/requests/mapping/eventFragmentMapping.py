from collections import Counter
from src.utils import listFileNames, readFile, saveToFile, listDirectories
from tabulate import tabulate


def createEventFragmentMapping():
    alwaysPresentKeys = ["lastUpdated", "creationTime", "self", "source", "time", "id", "text", "type"]
    c8y_data = readFile('c8y_data.json')
    eventTypesMapping = {device['id']: set() for device in c8y_data}

    for folder in listDirectories('events'):
        for fileName in listFileNames(folder):
            for device in readFile(fileName):
                deviceId = device['deviceId']
                if 'total' in device:
                    event = device['total']['event']
                    if event:
                        eventKeys = [key for key in event.keys() if key not in alwaysPresentKeys]
                        for key in eventKeys:
                            eventTypesMapping[deviceId].add(key)

                if 'eventByType' in device:
                    for eventByType in device['eventByType']:
                        event = eventByType['event']
                        eventKeys = [key for key in event.keys() if key not in alwaysPresentKeys]
                        for key in eventKeys:
                            eventTypesMapping[deviceId].add(key)

                if 'typeFragment' in device:
                    for eventByType in device['typeFragment']:
                        event = eventByType['event']
                        eventKeys = [key for key in event.keys() if key not in alwaysPresentKeys]
                        for key in eventKeys:
                            eventTypesMapping[deviceId].add(key)

    data = {key: sorted(value) for key, value in eventTypesMapping.items()}
    saveToFile(data, f'events/c8y_events_id_to_fragment_mapping.json')


def mappingOverview():
    jsonData = readFile("events/c8y_events_id_to_fragment_mapping.json")
    counter = Counter([tuple(value) for key, value in sorted(jsonData.items())])

    data = [(value, key) for key, value in counter.items()]
    table = tabulate(data, headers=["Count", "Fragments"], tablefmt="pipe")
    print(table)
