from tqdm import tqdm
from datetime import date

from .config import getCumulocityApi
from src.utils import tqdmFormat

c8y = getCumulocityApi()


def getTotalEvents(devices, dateFrom: date, dateTo: date):
    data = []
    for device in tqdm(devices, desc="Requesting event count", bar_format=tqdmFormat):

        if 'eventCount' in device and device['eventCount'] == -1:
            response = requestLastEvent(dateFrom, dateTo, device['id'])
            eventCount = response['count']
            lastEvent = response['lastEvent']
        else:
            lastEvent = device['lastEvent']
            eventCount = device['eventCount']

        if 'eventCountValidation' in device and device['eventCountValidation'] == -1:
            response = requestFirstEvent(dateFrom, dateTo, device['id'])
            eventCountValidation = response['count']
            firstEvent = response['firstEvent']
        else:
            firstEvent = device['lastEvent']
            eventCountValidation = device['eventCount']

        data.append({
            **device,
            'eventCount': eventCount,
            'lastEvent': lastEvent,
            'eventCountValidation': eventCountValidation,
            'firstEvent': firstEvent
        })
    return data


def requestLastEvent(dateFrom, dateTo, deviceId):
    additionalParameters = {'revert': 'false'}
    response = requestEventCount(dateFrom, dateTo, deviceId, additionalParameters)
    return {'count': response['count'], 'lastEvent': response['lastEvent']}


def requestFirstEvent(dateFrom, dateTo, deviceId):
    additionalParameters = {'revert': 'true'}
    response = requestEventCount(dateFrom, dateTo, deviceId, additionalParameters)
    return {'count': response['count'], 'firstEvent': response['lastEvent']}


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
            lastEvent = response['events'][0]
        else:
            lastEvent = {}
    except:
        lastEvent = {}
        eventCount = -1
    return {'count': eventCount, 'lastEvent': lastEvent}
