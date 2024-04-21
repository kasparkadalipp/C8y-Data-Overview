from tqdm import tqdm
from datetime import date

from .config import getCumulocityApi
from src.utils import tqdmFormat

c8y = getCumulocityApi()


def getTotalEvents(devices, dateFrom: date, dateTo: date):
    data = []
    for device in tqdm(devices, desc="Requesting event count", bar_format=tqdmFormat):

        if 'eventCount' in device and device['eventCount'] == -1:
            response = requestLatestEvent(dateFrom, dateTo, device['id'])
            eventCount = response['count']
            latestEvent = response['latestEvent']
        else:
            latestEvent = device['latestEvent']
            eventCount = device['eventCount']

        if 'eventCountValidation' in device and device['eventCountValidation'] == -1:
            response = requestOldestEvent(dateFrom, dateTo, device['id'])
            eventCountValidation = response['count']
            oldestEvent = response['oldestEvent']
        else:
            oldestEvent = device['latestEvent']
            eventCountValidation = device['eventCount']

        data.append({
            **device,
            'eventCount': eventCount,
            'latestEvent': latestEvent,
            'eventCountValidation': eventCountValidation,
            'oldestEvent': oldestEvent
        })
    return data


def requestLatestEvent(dateFrom, dateTo, deviceId):
    additionalParameters = {'revert': 'false'}
    response = requestEventCount(dateFrom, dateTo, deviceId, additionalParameters)
    return {'count': response['count'], 'latestEvent': response['latestEvent']}


def requestOldestEvent(dateFrom, dateTo, deviceId):
    additionalParameters = {'revert': 'true'}
    response = requestEventCount(dateFrom, dateTo, deviceId, additionalParameters)
    return {'count': response['count'], 'oldestEvent': response['latestEvent']}


def requestEventCount(dateFrom, dateTo, deviceId, additionalParameters):
    parameters = {
        'dateFrom': dateFrom.isoformat(),
        'dateTo': dateTo.isoformat(),
        'source': deviceId,
        'pageSize': 1,
        'currentPage': 1,
        'withTotalPages': 'true',
        'revert': 'false'
    }
    if additionalParameters:
        parameters.update(additionalParameters)

    try:
        response = c8y.get(resource="/event/events", params=parameters)
        eventCount = response['statistics']['totalPages']

        if eventCount > 0:
            latestEvent = response['events'][0]
        else:
            latestEvent = {}
    except:
        latestEvent = {}
        eventCount = -1
    return {'count': eventCount, 'latestEvent': latestEvent}
