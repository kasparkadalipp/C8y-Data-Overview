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
deviceIdMapping = {device['id']: device for device in c8y_data}

files = fileContentsFromFolder('../data/telia/events/total')

if pathExists('telia/c8y_events_id_to_type_mapping.json'):
    eventTypesMapping = readFile('telia/c8y_events_id_to_type_mapping.json')
else:
    eventTypesMapping = {device['id']: set() for device in c8y_data}
    for jsonFile in files:
        for device in jsonFile:
            deviceId = device['deviceId']
            event = device['total']['event']
            if event:
                eventTypesMapping[deviceId].add(event['type'])

def requestMissingValues(year, month, filePath):
    fileContents = readFile(filePath)

    missingValueCount = 0
    for device in fileContents:
        for eventType in device['eventByType']:
            if eventType['count'] < 0:
                missingValueCount += 1
    if missingValueCount == 0:
        return []

    c8y_events = []
    for savedEvents in tqdm(fileContents, desc=f"{calendar.month_abbr[month]} {year} - missing values", bar_format=tqdmFormat):
        device = deviceIdMapping[savedEvents['deviceId']]

        result = {
            "deviceId": device['id'],
            "deviceType": device['type'],
            "eventByType": []
        }

        for event in savedEvents['eventByType']:
            eventType = event['type']
            count = event['count']

            if count >= 0:
                result['eventByType'].append(event)
                continue

            response = MonthlyEvents(device, enforceBounds=True).requestAggregatedEventCountForType(year, month, eventType)

            result['eventByType'].append({
                "type": eventType,
                "count": response['count'],
                "event": response['event']
            })
        c8y_events.append(result)
    return c8y_events


def requestEventTypes(year, month):
    result = []
    for device in tqdm(c8y_data, desc=f"{calendar.month_abbr[month]} {year}", bar_format=tqdmFormat):
        deviceId = device['id']

        c8y_events = {
            "deviceId": deviceId,
            "deviceType": device['type'],
            "eventByType": []
        }

        for eventType in eventTypesMapping[deviceId]:
            response = MonthlyEvents(device, enforceBounds=True).requestEventCountForType(year, month, eventType)
            c8y_events['eventByType'].append({
                "type": eventType,
                "count": response['count'],
                "event": response['event']
            })
        result.append(c8y_events)
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
        data = requestMissingValues(year, month, filePath)
        if data:
            saveToFile(data, filePath, overwrite=True)
        else:
            print(f"{calendar.month_abbr[month]} {year} - skipped")
    else:
        data = requestEventTypes(year, month)
        saveToFile(data, filePath, overwrite=False)
    currentDate -= relativedelta(months=1)
