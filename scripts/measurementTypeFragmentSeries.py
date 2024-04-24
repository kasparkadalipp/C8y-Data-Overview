from datetime import date

from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

load_dotenv('../.env.telia')
from src.cumulocity import MonthlyMeasurements
from src.utils import tqdmFormat, saveToFile, pathExists, fileContentsFromFolder
from tqdm import tqdm
import json
import calendar
from dateutil.parser import parse

with open('../data/telia/c8y_data.json', 'r', encoding='utf8') as json_file:
    c8y_data = json.load(json_file)

files = fileContentsFromFolder('../data/telia/measurements/fragmentSeries')


def getMeasurementTypes(measurement: dict):
    result = set()
    measurementType = measurement['type']
    ignoredKeys = ["lastUpdated", "creationTime", "self", "source", "time", "id", "text"] + ['type']

    for fragment, fragmentObj in measurement.items():
        if fragment in ignoredKeys or fragmentObj is None: continue
        for series, seriesObj in fragmentObj.items():
            result.add((measurementType, fragment, series))
    return result


typeFragmentSeriesMapping = {device['id']: set() for device in c8y_data}

for jsonFile in files:
    for device in jsonFile:
        deviceId = device['deviceId']
        for fragmentSeries in device['fragmentSeries']:
            measurement = fragmentSeries['measurement']
            if measurement:
                typeFragmentSeries = getMeasurementTypes(measurement)
                typeFragmentSeriesMapping[deviceId] = typeFragmentSeriesMapping[deviceId].union(typeFragmentSeries)


def requestFragmentSeries(year, month):
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
        print(f"{calendar.month_abbr[month]} {year} - skipped")
    else:
        data = requestFragmentSeries(year, month)
        saveToFile(data, filePath, overwrite=False)
    currentDate -= relativedelta(months=1)
