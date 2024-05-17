from datetime import date

from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

load_dotenv('../.env')
from src.cumulocity import MonthlyMeasurements
from src.utils import tqdmFormat, saveToFile, pathExists, readFile
from tqdm import tqdm
import calendar
from dateutil.parser import parse

c8y_data = readFile('telia/c8y_data.json')
deviceIdMapping = {device['id']: device for device in c8y_data}


def requestMissingValues(year, month, filePath):
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


print(f'Oldest measurement {min([parse(d['oldestMeasurement']['time']).date() for d in c8y_data if d['oldestMeasurement']])}')
print(f'Latest measurement {max([parse(d['latestMeasurement']['time']).date() for d in c8y_data if d['latestMeasurement']])}')

startingDate = date(2014, 7, 1)
lastDate = date(2024, 3, 1)

currentDate = lastDate
while startingDate <= currentDate <= lastDate:
    year = currentDate.year
    month = currentDate.month

    filePath = f"telia/measurements/fragmentSeries/{MonthlyMeasurements.fileName(year, month)}"
    if pathExists(filePath):
        data = requestMissingValues(year, month, filePath)
        if data:
            saveToFile(data, filePath, overwrite=True)
        else:
            print(f"{calendar.month_abbr[month]} {year} - skipped")
    else:
        data = requestFragmentSeries(year, month)
        saveToFile(data, filePath, overwrite=False)
    currentDate -= relativedelta(months=1)
