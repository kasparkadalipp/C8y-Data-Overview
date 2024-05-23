import pandas as pd
from collections import defaultdict
from src.utils import listFileNames, readFile, getPath


def formatUnits(units):
    units = list(units)
    if len(units) == 1:
        return units[0]
    if len(units) == 0:
        return ''
    return units


def createFragmentSeriesSchema():
    folder = "measurements/fragmentSeries/"
    result = defaultdict(lambda: {'count': 0, 'units': set(), 'example': {}, 'values': set()})
    for fileName in listFileNames('../data/' + folder):
        for device in readFile(folder + fileName):
            deviceType = device['deviceType']

            for fragmentSeries in device['fragmentSeries']:
                fragment = fragmentSeries['fragment']
                series = fragmentSeries['series']
                count = fragmentSeries['count']

                measurement = fragmentSeries['measurement']
                if measurement:
                    measurementValue = measurement[fragment][series]
                    unit = measurementValue['unit'] if 'unit' in measurementValue else ''
                    value = measurementValue['value'] if 'value' in measurementValue else ''
                    key = (deviceType, fragment, series)
                    result[key]['units'].add(unit)
                    result[key]['count'] += count
                    result[key]['example'] = measurement
                    if len(result[key]['values']) < 10:
                        result[key]['values'].add(value)

    data = []
    for key, value in result.items():
        deviceType, fragment, series = key
        count = value['count']
        units = formatUnits(value['units'])
        exampleMeasurement = value['example']
        measurementValues = list(value['values'])

        row = {
            'deviceType': deviceType,
            'fragment': fragment,
            'series': series,
            'count': count,
            'units': units,
            'example values': measurementValues,
            'example measurement': exampleMeasurement
        }
        data.append(row)

    df = pd.DataFrame(data)
    df.to_csv(getPath('Measurement schema (fragment + series).csv'), index=False, encoding='utf-8-sig')


def createTypeFragmentSeriesSchema():
    folder = "measurements/typeFragmentSeries/"
    result = defaultdict(lambda: {'count': 0, 'units': set(), 'example': {}, 'values': set()})
    for fileName in listFileNames('../data/' + folder):
        for device in readFile(folder + fileName):
            deviceType = device['deviceType']

            for fragmentSeries in device['typeFragmentSeries']:
                measurementType = fragmentSeries['type']
                fragment = fragmentSeries['fragment']
                series = fragmentSeries['series']
                count = fragmentSeries['count']

                measurement = fragmentSeries['measurement']
                if measurement:
                    measurementValue = measurement[fragment][series]
                    unit = measurementValue['unit'] if 'unit' in measurementValue else ''
                    value = measurementValue['value'] if 'value' in measurementValue else ''
                    key = (deviceType, measurementType, fragment, series)
                    result[key]['units'].add(unit)
                    result[key]['count'] += count
                    result[key]['example'] = measurement
                    if len(result[key]['values']) < 10:
                        result[key]['values'].add(value)

    data = []
    for key, value in result.items():
        deviceType, measurementType, fragment, series = key
        count = value['count']
        units = formatUnits(value['units'])
        exampleMeasurement = value['example']
        measurementValues = list(value['values'])

        row = {
            'deviceType': deviceType,
            'measurementType': measurementType,
            'fragment': fragment,
            'series': series,
            'count': count,
            'units': units,
            'example values': measurementValues,
            'example measurement': exampleMeasurement
        }
        data.append(row)

    df = pd.DataFrame(data)
    df.to_csv(getPath('Measurement schema (type + fragment + series).csv'), index=False, encoding='utf-8-sig')
