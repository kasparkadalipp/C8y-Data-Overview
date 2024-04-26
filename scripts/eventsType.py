from dotenv import load_dotenv

load_dotenv('../.env.telia')
from src.cumulocity import MonthlyEvents
from src.utils import tqdmFormat, saveToFile, pathExists, fileContentsFromFolder, readFile
from tqdm import tqdm
import calendar
from datetime import date
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta

c8y_data = readFile('telia/c8y_data.json')
files = fileContentsFromFolder('../data/telia/events/total')

eventTypesMapping = {device['id']: set() for device in c8y_data}

for jsonFile in files:
    for device in jsonFile:
        deviceId = device['deviceId']
        event = device['total']['event']
        if event:
            eventTypesMapping[deviceId].add(event['type'])


def requestFragmentSeries(year, month):
    result = []
    for device in tqdm(c8y_data, desc=f"{calendar.month_abbr[month]} {year}", bar_format=tqdmFormat):
        deviceId = device['id']

        c8y_measurements = {
            "deviceId": deviceId,
            "deviceType": device['type'],
            "eventByType": []
        }

        for eventType in eventTypesMapping[deviceId]:
            response = MonthlyEvents(device, enforceBounds=True).requestEventCountForType(year, month, eventType)

            c8y_measurements['eventByType'].append({
                "type": eventType,
                "count": response['count'],
                "event": response['event']
            })
        result.append(c8y_measurements)
    return result


print(f'Oldest event {min([parse(d['oldestEvent']['time']).date() for d in c8y_data if d['oldestEvent']])}')
print(f'Latest event {max([parse(d['latestEvent']['time']).date() for d in c8y_data if d['latestEvent']])}')

startingDate = date(2016, 11, 1)
lastDate = date(2024, 3, 1)

currentDate = lastDate
while startingDate <= currentDate <= lastDate:
    year = currentDate.year
    month = currentDate.month

    filePath = f"telia/events/type/{MonthlyEvents.fileName(year, month)}"
    if pathExists(filePath):
        print(f"{calendar.month_abbr[month]} {year} - skipped")
    else:
        data = requestFragmentSeries(year, month)
        saveToFile(data, filePath, overwrite=False)
    currentDate -= relativedelta(months=1)
