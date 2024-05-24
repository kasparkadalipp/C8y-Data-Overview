from collections import Counter
from src.utils import listFileNames, readFile, saveToFile, listDirectories
from tabulate import tabulate


def createEventTypeMapping():
    c8y_data = readFile('c8y_data.json')
    eventTypesMapping = {device['id']: set() for device in c8y_data}

    for folder in listDirectories('events'):
        for fileName in listFileNames(folder):
            for device in readFile(fileName):
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
    saveToFile(data, f'events/events/c8y_events_id_to_fragment_mapping.json')


def mappingOverview():
    jsonData = readFile("events/events/c8y_events_id_to_fragment_mapping.json")
    counter = Counter([tuple(value) for key, value in sorted(jsonData.items())])

    data = [(value, key) for key, value in counter.items()]
    table = tabulate(data, headers=["Count", "Types"], tablefmt="pipe")
    print(table)
