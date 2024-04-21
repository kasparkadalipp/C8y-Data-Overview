from datetime import date

from .measurements import requestMonthBounds
from .config import getCumulocityApi
from dateutil.parser import parse

c8y = getCumulocityApi()


class Events:
    def __init__(self, device: dict, enforceBounds=False):
        self.enforceBounds = enforceBounds
        self.deviceId = device['id']
        self.deviceType = device['type']
        self.ignoredEvent = 'ignoredEvent' in device

        if enforceBounds:
            self.latestEvent = device['latestEvent']
            self.oldestEvent = device['oldestEvent']

    def hasEvents(self, dateFrom: date, dateTo: date) -> bool:
        if self.enforceBounds:
            if not self.latestEvent or not self.oldestEvent:
                return False
            latestDate = parse(self.latestEvent['time']).date()
            oldestDate = parse(self.oldestEvent['time']).date()
            if latestDate < dateFrom or dateTo < oldestDate:
                return False
        return True

    def requestLatestEvent(self, dateFrom: date, dateTo: date) -> dict:
        additionalParameters = {'revert': 'false'}
        response = self.requestEventCount(dateFrom, dateTo, additionalParameters)
        return {'count': response['count'], 'latestEvent': response['event']}

    def requestOldestEvent(self, dateFrom: date, dateTo: date) -> dict:
        additionalParameters = {'revert': 'true'}
        response = self.requestEventCount(dateFrom, dateTo, additionalParameters)
        return {'count': response['count'], 'oldestEvent': response['event']}

    def requestEventCount(self, dateFrom: date, dateTo: date, additionalParameters: dict = None) -> dict:
        parameters = {
            'dateFrom': dateFrom.isoformat(),
            'dateTo': dateTo.isoformat(),
            'source': self.deviceId,
            'pageSize': 1,
            'currentPage': 1,
            'withTotalPages': 'true',
            'revert': 'false'
        }
        if additionalParameters:
            parameters.update(additionalParameters)

        if self.ignoredEvent:
            eventCount = -2
            latestEvent = {}
        elif self.hasEvents(dateFrom, dateTo):
            try:
                response = c8y.get(resource="/event/events", params=parameters)
                eventCount = response['statistics']['totalPages']

                if eventCount > 0:
                    latestEvent = response['events'][0]
                else:
                    latestEvent = {}
            except KeyboardInterrupt:  # TODO better error handling
                raise KeyboardInterrupt
            except:
                latestEvent = {}
                eventCount = -1
        else:
            eventCount = 0
            latestEvent = {}

        return {'count': eventCount, 'event': latestEvent}


class MonthlyEvents:
    def __init__(self, device: dict, enforceBounds=True):
        self.cumulocity = Events(device, enforceBounds)

    def requestLatestEvent(self, year: int, month: int) -> dict:
        return self.cumulocity.requestLatestEvent(*requestMonthBounds(year, month))

    def requestOldestEvent(self, year: int, month: int) -> dict:
        return self.cumulocity.requestOldestEvent(*requestMonthBounds(year, month))

    def requestEventCount(self, year, month, additionalParameters: dict = None) -> dict:
        dateFrom, dateTo = requestMonthBounds(year, month)
        return self.cumulocity.requestEventCount(dateFrom, dateTo, additionalParameters)

    @staticmethod
    def fileName(year: int, month: int) -> str:
        dateFrom, dateTo = requestMonthBounds(year, month)
        return f'c8y_events {dateFrom} - {dateTo}.json'
