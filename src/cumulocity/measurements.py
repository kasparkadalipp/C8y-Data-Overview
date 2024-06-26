from .config import getCumulocityApi
from datetime import date
from typing import Tuple
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
import sys


class Measurements:
    def __init__(self, device: dict, enforceBounds=False):
        self.enforceBounds = enforceBounds
        self.deviceId = device['id']
        self.supportedFragmentAndSeries = device['c8y_supportedSeries']
        self.c8y = getCumulocityApi()

        if enforceBounds:
            self.latestMeasurement = device['latestMeasurement']
            self.oldestMeasurement = device['oldestMeasurement']

    def requestLatestMeasurement(self, dateFrom: date, dateTo: date) -> dict:
        additionalParameters = {'revert': 'true'}
        response = self.requestMeasurementCount(dateFrom, dateTo, additionalParameters)
        return {'count': response['count'], 'latestMeasurement': response['measurement']}

    def requestOldestMeasurement(self, dateFrom: date, dateTo: date) -> dict:
        additionalParameters = {'revert': 'false'}
        response = self.requestMeasurementCount(dateFrom, dateTo, additionalParameters)
        return {'count': response['count'], 'oldestMeasurement': response['measurement']}

    def requestFragmentSeriesCount(self, dateFrom: date, dateTo: date, fragment: str, series: str) -> dict:
        additionalParameters = {'valueFragmentType': fragment, 'valueFragmentSeries': series}
        return self.requestMeasurementCount(dateFrom, dateTo, additionalParameters)

    def requestTypeFragmentSeriesCount(self, dateFrom, dateTo, measurementType: str, fragment: str, series: str) -> dict:
        additionalParameters = {'type': measurementType, 'valueFragmentType': fragment, 'valueFragmentSeries': series}
        return self.requestMeasurementCount(dateFrom, dateTo, additionalParameters)

    def hasMeasurements(self, dateFrom: date, dateTo: date) -> bool:
        if self.latestMeasurement:
            latestDate = parse(self.latestMeasurement['time']).date()
            if latestDate < dateFrom:
                return False
        if self.oldestMeasurement:
            oldestDate = parse(self.oldestMeasurement['time']).date()
            if dateTo < oldestDate:
                return False
        return True

    def requestMeasurementCount(self, dateFrom: date, dateTo: date, additionalParameters: dict = None) -> dict:
        parameters = {
            'dateFrom': dateFrom.isoformat(),
            'dateTo': dateTo.isoformat(),
            'source': self.deviceId,
            'pageSize': 1,
            'currentPage': 1,
            'withTotalPages': 'true',
            'revert': 'false',
        }
        if additionalParameters:
            parameters.update(additionalParameters)

        if self.enforceBounds and not self.hasMeasurements(dateFrom, dateTo):
            return {'count': 0, 'measurement': {}}

        try:
            response = self.c8y.get(resource="/measurement/measurements", params=parameters)
            measurementCount = response['statistics']['totalPages']
            latestMeasurement = response['measurements']
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except Exception as e:
            if "Status: 401" in str(e):
                sys.exit("Invalid cumulocity tenant credentials.")
            measurementCount = -1
            latestMeasurement = {}

        if latestMeasurement:
            latestMeasurement = latestMeasurement[0]
            del latestMeasurement['self']
            del latestMeasurement['source']['self']
        return {'count': measurementCount, 'measurement': latestMeasurement}

    def requestLatestMeasurementValidation(self, dateTo: date) -> dict:
        try:
            return self.c8y.measurements.get_last(source=self.deviceId, before=dateTo).to_json()
        except IndexError:
            return {}


class MonthlyMeasurements:
    def __init__(self, device: dict, enforceBounds=True):
        self.cumulocity = Measurements(device, enforceBounds)

    def requestLatestMeasurement(self, year: int, month: int) -> dict:
        return self.cumulocity.requestLatestMeasurement(*requestMonthBounds(year, month))

    def requestOldestMeasurement(self, year: int, month: int) -> dict:
        return self.cumulocity.requestOldestMeasurement(*requestMonthBounds(year, month))

    def requestFragmentSeriesCount(self, year: int, month: int, fragment: str, series: str) -> dict:
        dateFrom, dateTo = requestMonthBounds(year, month)
        return self.cumulocity.requestFragmentSeriesCount(dateFrom, dateTo, fragment, series)

    def requestTypeFragmentSeriesCount(self, year: int, month: int, measurementType: str, fragment: str, series: str) -> dict:
        dateFrom, dateTo = requestMonthBounds(year, month)
        return self.cumulocity.requestTypeFragmentSeriesCount(dateFrom, dateTo, measurementType, fragment, series)

    def requestMeasurementCount(self, year, month, additionalParameters: dict = None) -> dict:
        dateFrom, dateTo = requestMonthBounds(year, month)
        return self.cumulocity.requestMeasurementCount(dateFrom, dateTo, additionalParameters)

    def requestAggregatedFragmentSeriesCount(self, year: int, month: int, fragment: str, series: str) -> dict:
        additionalParameters = {'valueFragmentType': fragment, 'valueFragmentSeries': series}
        return self.requestAggregatedMeasurementCount(year, month, additionalParameters)

    def requestAggregatedTypeFragmentSeriesCount(self, year: int, month: int, measurementType: str, fragment: str, series: str) -> dict:
        additionalParameters = {'type': measurementType, 'valueFragmentType': fragment, 'valueFragmentSeries': series}
        return self.requestAggregatedMeasurementCount(year, month, additionalParameters)

    def requestAggregatedMeasurementCount(self, year: int, month: int, additionalParameters: dict = None) -> dict:
        dateFrom, dateTo = requestMonthBounds(year, month)
        result = {'measurement': {}, 'count': 0}

        periodDays = 3
        startingDate = dateFrom
        endingDate = startingDate + relativedelta(days=periodDays)
        while startingDate < dateTo:
            response = self.cumulocity.requestMeasurementCount(startingDate, endingDate, additionalParameters)
            result['count'] += response['count']
            if response['measurement']:
                result['measurement'] = response['measurement']
            if response['count'] < 0:
                return {'measurement': {}, 'count': -1}
            startingDate += relativedelta(days=periodDays)
            endingDate = min(endingDate + relativedelta(days=periodDays), dateTo)
        return result

    @staticmethod
    def fileName(year: int, month: int) -> str:
        dateFrom, dateTo = requestMonthBounds(year, month)
        return f'c8y_measurements {dateFrom} - {dateTo}.json'


def requestMonthBounds(year: int, month: int) -> Tuple[date, date]:
    inclusiveDateFrom = date(year, month, 1)
    exclusiveDateTo = inclusiveDateFrom + relativedelta(months=1)
    return inclusiveDateFrom, exclusiveDateTo
