import json
from dateutil.parser import parse
import os

with open('../data/telia/c8y_data.json', 'r', encoding='utf8') as json_file:
    c8y_data = json.load(json_file)

basePath = "../data/telia/"


def getFilePaths(folder):
    path = basePath + folder
    if not os.path.exists(path):
        return []
    fileNames = os.listdir(path)
    return [path + file for file in fileNames]


def getFileContents(filePath):
    with open(filePath, 'r', encoding='utf8') as json_file:
        return json.load(json_file)


def getFileContentsByFolder(folder):
    return [getFileContents(filePath) for filePath in getFilePaths(folder)]


def statistics():
    print('Data from 1970-01-01 to 2024-04-01')
    print(f'Total events : {sum([device['eventCount'] for device in c8y_data]):,}')
    print(f'Total measurements : {sum([device['measurementCount'] for device in c8y_data]):,}')
    print(
        f'Devices that send measurements {sum([bool(device['latestMeasurement']) for device in c8y_data]) / len(c8y_data) * 100:.2f}%')
    print(
        f'Devices that send events {sum([bool(device['latestEvent']) for device in c8y_data]) / len(c8y_data) * 100:.2f}%')
    print(
        f'Oldest event {min([parse(device['oldestEvent']['time']).date() for device in c8y_data if device['oldestEvent']])}')
    print(
        f'Latest event {max([parse(device['latestEvent']['time']).date() for device in c8y_data if device['latestEvent']])}')
    print(
        f'Oldest measurement {min([parse(device['oldestMeasurement']['time']).date() for device in c8y_data if device['oldestMeasurement']])}')
    print(
        f'Latest measurement {max([parse(device['latestMeasurement']['time']).date() for device in c8y_data if device['latestMeasurement']])}')


def monthlyMeasurementTotal():
    monthlyFileContents = getFileContentsByFolder('measurements/total/')
    total = 0
    for file in monthlyFileContents:
        total += sum([measurement['total']['count'] for measurement in file])
    print(f'Total measurements from monthly measurement: {total:,}')


def monthlyMeasurementFragmentSeries():
    monthlyFileContents = getFileContentsByFolder('measurements/fragmentSeries/')
    total = 0
    for file in monthlyFileContents:
        for device in file:
            for fragmentSeries in device['fragmentSeries']:
                total += fragmentSeries['count']
    print(f'Total measurements from monthly fragment + series: {total:,}')


def monthlyMeasurementFragmentSeriesType():
    monthlyFileContents = getFileContentsByFolder('measurements/typeFragmentSeries/')
    total = 0
    for file in monthlyFileContents:
        for device in file:
            for fragmentSeries in device['typeFragmentSeries']:
                total += fragmentSeries['count']
    print(f'Total measurements from monthly fragment + series + type: {total:,}')


def monthlyEventTotal():
    monthlyFileContents = getFileContentsByFolder('events/total/')
    total = 0
    for file in monthlyFileContents:
        total += sum([device['total']['count'] for device in file])
    print(f'Total events from monthly events: {total:,}')


def monthlyEventType():
    monthlyFileContents = getFileContentsByFolder('events/type/')
    total = 0
    for file in monthlyFileContents:
        for device in file:
            for event in device['eventByType']:
                total += event['count']
    print(f'Total events from monthly event types: {total:,}')


def monthlyEventTypeFragment():
    monthlyFileContents = getFileContentsByFolder('events/typeFragment/')
    total = 0
    for file in monthlyFileContents:
        for device in file:
            for event in device['typeFragment']:
                total += event['count']
    print(f'Total events from monthly event types + fragments: {total:,}')


statistics()
monthlyMeasurementTotal()
monthlyMeasurementFragmentSeries()
monthlyMeasurementFragmentSeriesType()
monthlyEventTotal()
monthlyEventType()
monthlyEventTypeFragment()
