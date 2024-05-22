import calendar
from datetime import date
from dateutil.relativedelta import relativedelta
from src.cumulocity.requests.mapping.eventTypeMapping import createEventTypeMapping
from src.cumulocity import MonthlyEvents
from src.utils import tqdmFormat, saveToFile, pathExists, readFile
from tqdm import tqdm


def requestMissingValues(year, month, filePath):
    c8y_data = readFile('c8y_data.json')
    deviceIdMapping = {device['id']: device for device in c8y_data}
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
    c8y_data = readFile('c8y_data.json')
    if not pathExists('c8y_events_id_to_type_mapping.json'):
        createEventTypeMapping()
    eventTypesMapping = readFile('c8y_events_id_to_type_mapping.json')

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


def requestMonthlyData(startingDate: date, lastDate: date):
    if startingDate <= lastDate:
        raise ValueError("Last date can't be before starting date")

    startingDate = startingDate.replace(day=1)
    lastDate = lastDate.replace(day=1)
    currentDate = startingDate
    while lastDate <= currentDate <= startingDate:
        year = currentDate.year
        month = currentDate.month

        filePath = f"events/type/{MonthlyEvents.fileName(year, month)}"
        fileExists = pathExists(filePath)

        if not fileExists:
            data = requestEventTypes(year, month)
            saveToFile(data, filePath)

        data = requestMissingValues(year, month, filePath)
        if data:
            saveToFile(data, filePath)
        elif fileExists:
            print(f"{calendar.month_abbr[month]} {year} - skipped")

        currentDate -= relativedelta(months=1)
