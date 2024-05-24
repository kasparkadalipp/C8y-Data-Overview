import calendar
from datetime import date
from dateutil.relativedelta import relativedelta
from src.cumulocity.requests.mapping.eventFragmentMapping import createEventFragmentMapping
from src.cumulocity.requests.mapping.eventTypeMapping import createEventTypeMapping
from src.cumulocity import MonthlyEvents
from src.utils import tqdmFormat, saveToFile, pathExists, readFile, ensureFileAndRead
from tqdm import tqdm


def requestEventTypes(year, month):
    c8y_data = readFile('c8y_data.json')
    typeMapping = ensureFileAndRead('events/c8y_events_id_to_type_mapping.json', createEventTypeMapping)
    fragmentMapping = ensureFileAndRead('events/c8y_events_id_to_fragment_mapping.json', createEventFragmentMapping)

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
                while response['count'] < 0:
                    response = MonthlyEvents(device, enforceBounds=True).requestAggregatedEventCountForTypeFragment(year, month, eventType, fragment)

                c8y_events['typeFragment'].append({
                    "type": eventType,
                    "fragment": fragment,
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

        filePath = f"events/typeFragment/{MonthlyEvents.fileName(year, month)}"

        if pathExists(filePath):
            print(f"{calendar.month_abbr[month]} {year} - skipped")
        else:
            data = requestEventTypes(year, month)
            saveToFile(data, filePath)

        currentDate -= relativedelta(months=1)
