from tqdm import tqdm
from datetime import date

from .config import getCumulocityApi
from src.utils import tqdmFormat


def getEventCount(devices, dateFrom: date, dateTo: date):
    c8y = getCumulocityApi()

    data = []
    for deviceObj in tqdm(devices, desc="Requesting event count", bar_format=tqdmFormat):
        device = deviceObj['device']
        deviceId = device['id']

        parameters = {
            'dateFrom': dateFrom.isoformat(),
            'dateTo': dateTo.isoformat(),
            'source': deviceId,
            'pageSize': 1,
            'currentPage': 1,
            'withTotalPages': 'true'
        }

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

        data.append({
            **deviceObj,
            'eventCount': eventCount,
            'lastEvent': lastEvent
        })
    return data