import calendar
from datetime import date
from dateutil.relativedelta import relativedelta
from src.cumulocity.requests.mapping.eventTypeMapping import createEventTypeMapping
from src.cumulocity import MonthlyEvents, getCumulocityApi
from src.utils import tqdmFormat, saveToFile, pathExists, readFile, ensureFileAndRead
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
    eventTypesMapping = ensureFileAndRead('events/c8y_events_id_to_type_mapping.json', createEventTypeMapping)

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


def requestMissingTypesForMonth(year, month):
    validationFilePath = f"events/total/{MonthlyEvents.fileName(year, month)}"
    filePath = f"events/type/{MonthlyEvents.fileName(year, month)}"
    typeMapping = ensureFileAndRead('events/c8y_events_id_to_type_mapping.json', createEventTypeMapping)

    c8y = getCumulocityApi()
    validation = {item['deviceId']: item['total']['count'] for item in readFile(validationFilePath)}

    c8y_events = []
    newTypes = False
    for device in readFile(filePath):
        deviceId = device['deviceId']
        currentCount = 0
        expectedCount = validation[deviceId]
        for event in device['eventByType']:
            currentCount += event['count']
        if currentCount == expectedCount:
            c8y_events.append(device)
            continue

        knownTypes = set(typeMapping[deviceId])
        generator = c8y.events.select(source=deviceId, page_size=1000)
        for event in generator:
            eventType = event.type
            if eventType not in knownTypes:
                response = MonthlyEvents({'id': deviceId}, enforceBounds=False).requestEventCountForType(year, month,
                                                                                                         eventType)
                while response['count'] < 0:
                    response = MonthlyEvents({'id': deviceId}, enforceBounds=False).requestAggregatedEventCountForType(
                        year, month, eventType)

                device['eventByType'].append({
                    "type": eventType,
                    "count": response['count'],
                    "event": response['event']
                })
                currentCount += response['count']
                knownTypes.add(eventType)
                typeMapping[deviceId].append(eventType)
                newTypes = True
            if currentCount >= expectedCount:
                break
        c8y_events.append(device)

    if newTypes:
        saveToFile(typeMapping, 'events/c8y_events_id_to_type_mapping.json')
        saveToFile(c8y_events, filePath)


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
        requestMissingTypesForMonth(year, month)

        currentDate -= relativedelta(months=1)
