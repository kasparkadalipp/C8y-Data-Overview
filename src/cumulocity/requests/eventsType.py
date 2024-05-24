import calendar
from datetime import date
from dateutil.relativedelta import relativedelta
from src.cumulocity.requests.mapping.eventTypeMapping import createEventTypeMapping
from src.cumulocity import MonthlyEvents, getCumulocityApi, requestMonthBounds
from src.utils import tqdmFormat, saveToFile, pathExists, readFile, ensureFileAndRead
from tqdm import tqdm


def requestEventTypes(year, month):
    c8y_data = readFile('c8y_data.json')
    eventTypesMapping = ensureFileAndRead('events/c8y_events_id_to_type_mapping.json', createEventTypeMapping)
    validation = {item['deviceId']: item['total']['count'] for item in readFile(f"events/total/{MonthlyEvents.fileName(year, month)}")}
    c8y = getCumulocityApi()

    result = []
    updateTypes = False
    for device in tqdm(c8y_data, desc=f"{calendar.month_abbr[month]} {year}", bar_format=tqdmFormat):
        deviceId = device['id']
        count = 0

        c8y_events = {
            "deviceId": deviceId,
            "deviceType": device['type'],
            "eventByType": []
        }
        knownTypes = set(eventTypesMapping[deviceId])
        generator = None
        while count < validation[deviceId]:
            if knownTypes:
                eventType = knownTypes.pop()
            else:
                eventType = None
                if generator is None:
                    dateFrom, dateTo = requestMonthBounds(year, month)
                    generator = c8y.events.select(source=deviceId, page_size=1000, after=dateFrom, before=dateTo)
                for event in generator:
                    if event.type not in eventTypesMapping[deviceId]:
                        eventTypesMapping[deviceId].append(event.type)
                        eventType = event.type
                        updateTypes = True
                        break
                if eventType is None:
                    break

            response = MonthlyEvents(device).requestEventCountForType(year, month, eventType)
            while response['count'] < 0:
                response = MonthlyEvents(device).requestAggregatedEventCountForType(
                    year, month, eventType)

            count += response['count']
            c8y_events['eventByType'].append({
                "type": eventType,
                "count": response['count'],
                "event": response['event']
            })
        result.append(c8y_events)

    if updateTypes:
        saveToFile(eventTypesMapping, 'events/c8y_events_id_to_type_mapping.json')
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

        if pathExists(filePath):
            print(f"{calendar.month_abbr[month]} {year} - skipped")
        else:
            data = requestEventTypes(year, month)
            saveToFile(data, filePath)

        currentDate -= relativedelta(months=1)
