from datetime import date

from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

load_dotenv('../.env.telia')

from src.cumulocity import MonthlyMeasurements
from src.utils import tqdmFormat, saveToFile, pathExists
from tqdm import tqdm
import json
import calendar
from dateutil.parser import parse

with open('../data/telia/c8y_data.json', 'r', encoding='utf8') as json_file:
    c8y_data = json.load(json_file)

def requestTotal(year, month):
    c8y_measurements = []
    for device in tqdm(c8y_data, desc=f"{calendar.month_abbr[month]} {year}",
                       bar_format=tqdmFormat):
        response = MonthlyMeasurements(device, enforceBounds=True).requestMeasurementCount(year, month)
        c8y_measurements.append({
            "deviceId": device['id'],
            "deviceType": device['type'],
            "total": {
                "count": response['count'],
                "measurement": response['measurement']
            }
        })
    return c8y_measurements


print(f'Oldest measurement {min([parse(d['oldestMeasurement']['time']).date() for d in c8y_data if d['oldestMeasurement']])}')
print(f'Latest measurement {max([parse(d['latestMeasurement']['time']).date() for d in c8y_data if d['latestMeasurement']])}')

startingDate = date(2014, 1, 1)
lastDate = date(2024, 3, 1)

while startingDate <= lastDate:
    year = startingDate.year
    month = startingDate.month

    filePath = f"telia/measurements/total/{MonthlyMeasurements.fileName(year, month)}"
    if pathExists(filePath):
        print(f"{calendar.month_abbr[month]} {year} - skipped")
    else:
        data = requestTotal(year, month)
        saveToFile(data, filePath, overwrite=False)

    startingDate += relativedelta(months=1)
