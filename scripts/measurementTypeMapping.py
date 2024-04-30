import os
from src.utils import fileNamesInFolder, readFile
from src.utils import readFileContents, saveToFile
from collections import Counter
from tabulate import tabulate

def listDirectories(path):
    entries = os.listdir(path)
    directories = [entry for entry in entries if os.path.isdir(os.path.join(path, entry))]
    return directories


def createMeasurementMapping():
    c8y_data = readFile('telia/c8y_data.json')
    eventTypesMapping = {device['id']: set() for device in c8y_data}

    for folder in listDirectories('../data/telia/measurements'):
        for file in fileNamesInFolder(f'../data/telia/measurements/{folder}'):
            jsonFile = readFileContents(f'../data/telia/measurements/{folder}/{file}')
            for device in jsonFile:
                deviceId = device['deviceId']
                if 'total' in device:
                    measurement = device['total']['measurement']
                    if measurement:
                        eventTypesMapping[deviceId].add(measurement['type'])

                if 'fragmentSeries' in device:
                    for fragmentSeries in device['fragmentSeries']:
                        measurement = fragmentSeries['measurement']
                        if measurement:
                            eventTypesMapping[deviceId].add(measurement['type'])

                if 'typeFragmentSeries' in device:
                    for typeFragmentSeries in device['typeFragmentSeries']:
                        measurement = typeFragmentSeries['measurement']
                        if measurement:
                            eventTypesMapping[deviceId].add(measurement['type'])

    data = {key: sorted(value) for key, value in eventTypesMapping.items()}
    saveToFile(data, "telia/c8y_measurements_id_to_type_mapping.json", overwrite=True)


def mappingOverview():
    jsonData = readFile("../data/telia/c8y_measurements_id_to_type_mapping.json")
    counter = Counter([tuple(value) for key, value in jsonData.items() if value])

    data = [(value, key) for key, value in counter.items()]
    table = tabulate(data, headers=["Count", "Types"], tablefmt="pipe")
    print(table)
