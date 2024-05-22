import calendar
from datetime import date
from dateutil.relativedelta import relativedelta
from src.cumulocity import MonthlyMeasurements
from src.utils import tqdmFormat, saveToFile, pathExists, readFile, YearMonthDate
from tqdm import tqdm


def requestMissingValues(year, month, filePath):
    c8y_data = readFile(f'c8y_data.json')
    deviceIdMapping = {device['id']: device for device in c8y_data}
    fileContents = readFile(filePath)

    missingValueCount = 0
    for device in fileContents:
        for fragmentSeries in device['fragmentSeries']:
            if fragmentSeries['count'] < 0:
                missingValueCount += 1
    if missingValueCount == 0:
        return []

    c8y_measurements = []
    for savedMeasurement in tqdm(readFile(filePath), desc=f"{calendar.month_abbr[month]} {year}",
                                 bar_format=tqdmFormat):
        device = deviceIdMapping[savedMeasurement['deviceId']]

        result = {
            "deviceId": device['id'],
            "deviceType": device['type'],
            "fragmentSeries": []
        }

        for measurement in savedMeasurement['fragmentSeries']:
            fragment = measurement['fragment']
            series = measurement['series']
            count = measurement['count']

            if count >= 0:
                result['fragmentSeries'].append(measurement)
                continue

            response = MonthlyMeasurements(device, enforceBounds=True).requestAggregatedFragmentSeriesCount(year, month, fragment, series)
            result['fragmentSeries'].append({
                "fragment": fragment,
                "series": series,
                "count": response['count'],
                "measurement": response['measurement']
            })
        c8y_measurements.append(result)
    return c8y_measurements


def requestFragmentSeries(year, month):
    c8y_data = readFile(f'c8y_data.json')
    result = []
    for device in tqdm(c8y_data, desc=f"{calendar.month_abbr[month]} {year}",
                       bar_format=tqdmFormat):

        c8y_measurements = {
            "deviceId": device['id'],
            "deviceType": device['type'],
            "fragmentSeries": []
        }

        for fragmentSeries in device['c8y_supportedSeries']:
            fragment = fragmentSeries['fragment']
            series = fragmentSeries['series']

            response = MonthlyMeasurements(device, enforceBounds=True).requestFragmentSeriesCount(year, month, fragment, series)
            c8y_measurements['fragmentSeries'].append({
                "fragment": fragment,
                "series": series,
                "count": response['count'],
                "measurement": response['measurement']
            })
        result.append(c8y_measurements)
    return result


def requestMonthlyData(startingDate: YearMonthDate, lastDate: YearMonthDate):
    if startingDate <= lastDate:
        raise ValueError("Last date can't be before starting date")

    startingDate = startingDate.replace(day=1)
    lastDate = lastDate.replace(day=1)
    currentDate = startingDate
    while lastDate <= currentDate <= startingDate:
        year = currentDate.year
        month = currentDate.month

        filePath = f"measurements/fragmentSeries/{MonthlyMeasurements.fileName(year, month)}"
        fileExists = pathExists(filePath)

        if not fileExists:
            data = requestFragmentSeries(year, month)
            saveToFile(data, filePath)

        data = requestMissingValues(year, month, filePath)
        if data:
            saveToFile(data, filePath)
        elif fileExists:
            print(f"{calendar.month_abbr[month]} {year} - skipped")

        currentDate -= relativedelta(months=1)
