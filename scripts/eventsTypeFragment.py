from dotenv import load_dotenv


load_dotenv('../.env.telia')
from src.cumulocity import MonthlyEvents
from src.utils import tqdmFormat, saveToFile, pathExists, fileContentsFromFolder, readFile
from tqdm import tqdm
import calendar
from datetime import date
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from eventFragmentMapping import createEventFragmentMapping
from eventTypeMapping import createEventTypeMapping


c8y_data = readFile('telia/c8y_data.json')
deviceIdMapping = {device['id']: device for device in c8y_data}

if not pathExists('telia/c8y_events_id_to_type_mapping.json'):
    createEventTypeMapping()
typeMapping = readFile('telia/c8y_events_id_to_type_mapping.json')

if not pathExists('telia/c8y_events_id_to_fragment_mapping.json'):
    createEventFragmentMapping()
fragmentMapping = readFile('telia/c8y_events_id_to_fragment_mapping.json')


def requestMissingValues(year, month, filePath):
    fileContents = readFile(filePath)

    missingValueCount = 0
    for device in fileContents:
        for eventTypeFragment in device['typeFragment']:
            if eventTypeFragment['count'] < 0:
                missingValueCount += 1
    if missingValueCount == 0:
        return []

    c8y_events = []
    for savedEvents in tqdm(fileContents, desc=f"{calendar.month_abbr[month]} {year} - missing values", bar_format=tqdmFormat):
        device = deviceIdMapping[savedEvents['deviceId']]

        result = {
            "deviceId": device['id'],
            "deviceType": device['type'],
            "typeFragment": []
        }

        for event in savedEvents['typeFragment']:
            eventType = event['type']
            fragment = event['fragment']
            count = event['count']

            if count >= 0:
                result['typeFragment'].append(event)
                continue

            response = MonthlyEvents(device, enforceBounds=True).requestAggregatedEventCountForType(year, month, eventType)

            result['typeFragment'].append({
                "type": eventType,
                "fragment": fragment,
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
            "typeFragment": []
        }

        for eventType in typeMapping[deviceId]:
            for fragment in fragmentMapping[deviceId]:
                response = MonthlyEvents(device, enforceBounds=True).requestEventCountForTypeFragment(year, month, eventType, fragment)
                c8y_events['typeFragment'].append({
                    "type": eventType,
                    "fragment": fragment,
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

    filePath = f"telia/events/typeFragment/{MonthlyEvents.fileName(year, month)}"
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
