from dateutil.parser import parse
from src.utils import readFile, listFileNames


def statistics():
    c8y_data = readFile('c8y_data.json')

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
    total = 0
    for fileName in listFileNames('measurements/total/'):
        total += sum([measurement['total']['count'] for measurement in readFile(fileName)])
    print(f'Total measurements from monthly measurement: {total:,}')


def monthlyMeasurementFragmentSeries():
    total = 0
    for fileName in listFileNames('measurements/fragmentSeries/'):
        for device in readFile(fileName):
            for fragmentSeries in device['fragmentSeries']:
                total += fragmentSeries['count']
    print(f'Total measurements from monthly fragment + series: {total:,}')


def monthlyMeasurementFragmentSeriesType():
    total = 0
    for fileName in listFileNames('measurements/typeFragmentSeries/'):
        for device in readFile(fileName):
            for fragmentSeries in device['typeFragmentSeries']:
                total += fragmentSeries['count']
    print(f'Total measurements from monthly fragment + series + type: {total:,}')


def monthlyEventTotal():
    total = 0
    for fileName in listFileNames('events/total/'):
        total += sum([device['total']['count'] for device in readFile(fileName)])
    print(f'Total events from monthly events: {total:,}')


def monthlyEventType():
    total = 0
    for fileName in listFileNames('events/type/'):
        for device in readFile(fileName):
            for event in device['eventByType']:
                total += event['count']
    print(f'Total events from monthly event types: {total:,}')


def monthlyEventTypeFragment():
    total = 0
    for fileName in listFileNames('events/typeFragment/'):
        for device in readFile(fileName):
            for event in device['typeFragment']:
                total += event['count']
    print(f'Total events from monthly event types + fragments: {total:,}')
