from datetime import date

from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

load_dotenv('../.env.telia')
from src.cumulocity import MonthlyMeasurements
from src.utils import tqdmFormat, saveToFile, pathExists, fileContentsFromFolder, readFile
from tqdm import tqdm
import calendar
from dateutil.parser import parse
from measurementTypeMapping import createMeasurementMapping

c8y_data = readFile('telia/c8y_data.json')
deviceIdMapping = {device['id']: device for device in c8y_data}


def getMeasurementTypes(measurement: dict):
    result = set()
    measurementType = measurement['type']
    ignoredKeys = ["lastUpdated", "creationTime", "self", "source", "time", "id", "text"] + ['type']

    for fragment, fragmentObj in measurement.items():
        if fragment in ignoredKeys or fragmentObj is None: continue
        for series, seriesObj in fragmentObj.items():
            result.add((measurementType, fragment, series))
    return result


if not pathExists('telia/c8y_measurements_id_to_type_mapping.json'):
    createMeasurementMapping()
typeFragmentSeriesMapping = readFile('telia/c8y_measurements_id_to_type_mapping.json')

def requestMissingValues(year, month, filePath):
    fileContents = readFile(filePath)

    missingValueCount = 0
    for device in fileContents:
        for typeFragmentSeries in device['typeFragmentSeries']:
            if typeFragmentSeries['count'] < 0:
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
            "typeFragmentSeries": []
        }

        for measurement in savedMeasurement['typeFragmentSeries']:
            measurementType = measurement['type']
            fragment = measurement['fragment']
            series = measurement['series']
            count = measurement['count']

            if count >= 0:
                result['typeFragmentSeries'].append(measurement)
                continue

            response = MonthlyMeasurements(device, enforceBounds=True).requestAggregatedTypeFragmentSeriesCount(year, month, measurementType, fragment, series)
            result['typeFragmentSeries'].append({
                "type": measurementType,
                "fragment": fragment,
                "series": series,
                "count": response['count'],
                "measurement": response['measurement']
            })
        c8y_measurements.append(result)
    return c8y_measurements



def requestTypeFragmentSeries(year, month):
    result = []
    for device in tqdm(c8y_data, desc=f"{calendar.month_abbr[month]} {year}", bar_format=tqdmFormat):
        deviceId = device['id']

        c8y_measurements = {
            "deviceId": deviceId,
            "deviceType": device['type'],
            "typeFragmentSeries": []
        }

        for measurementType, fragment, series in typeFragmentSeriesMapping[deviceId]:
            response = MonthlyMeasurements(device, enforceBounds=True).requestTypeFragmentSeriesCount(year, month,
                           measurementType, fragment, series)

            c8y_measurements['typeFragmentSeries'].append({
                "type": measurementType,
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

    filePath = f"telia/measurements/typeFragmentSeries/{MonthlyMeasurements.fileName(year, month)}"
    if pathExists(filePath):
        data = requestMissingValues(year, month, filePath)
        if data:
            saveToFile(data, filePath, overwrite=True)
        else:
            print(f"{calendar.month_abbr[month]} {year} - skipped")
    else:
        data = requestTypeFragmentSeries(year, month)
        saveToFile(data, filePath, overwrite=False)
    currentDate -= relativedelta(months=1)
