from datetime import date

from .config import getCumulocityApi
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta

c8y = getCumulocityApi()


class Measurements:
    def __init__(self, device: dict, enforceBounds=True):
        self.enforceBounds = enforceBounds
        self.deviceId = device['id']
        self.deviceType = device['type']
        self.supportedFragmentAndSeries = device['c8y_supportedSeries']

        if enforceBounds:
            if device['latestMeasurement']:
                self.latestMeasurement = parse(device['latestMeasurement']['time']).date()
            if device['oldestMeasurement']:
                self.oldestMeasurement = parse(device['oldestMeasurement']['time']).date()

    def hasMeasurements(self, dateFrom: date, dateTo: date) -> bool:
        if not self.supportedFragmentAndSeries:
            return False
        if self.enforceBounds:
            if not self.latestMeasurement or not self.oldestMeasurement:
                return False
            if self.latestMeasurement < dateFrom or dateTo < self.oldestMeasurement:
                return False
        return True

    def requestLatestMeasurement(self, dateFrom: date, dateTo: date):
        additionalParameters = {'revert': 'true'}
        response = self.requestMeasurementCount(dateFrom, dateTo, additionalParameters)
        return {'count': response['count'], 'latestMeasurement': response['measurement']}

    def requestOldestMeasurement(self, dateFrom: date, dateTo: date):
        additionalParameters = {'revert': 'false'}
        response = self.requestMeasurementCount(dateFrom, dateTo, additionalParameters)
        return {'count': response['count'], 'oldestMeasurement': response['measurement']}

    def requestMeasurementCount(self, dateFrom: date, dateTo: date, additionalParameters: dict = None):
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

        if self.hasMeasurements(dateFrom, dateTo):
            try:
                response = c8y.get(resource="/measurement/measurements", params=parameters)
                measurementCount = response['statistics']['totalPages']
                latestMeasurement = response['measurements']
            except:
                measurementCount = -1
                latestMeasurement = {}
        else:
            measurementCount = 0
            latestMeasurement = {}

        if latestMeasurement:
            latestMeasurement = latestMeasurement[0]
            del latestMeasurement['self']
            del latestMeasurement['source']['self']
        return {'count': measurementCount, 'measurement': latestMeasurement}

    def requestLatestMeasurementValidation(self, dateTo: date):
        try:
            return c8y.measurements.get_last(source=self.deviceId, before=dateTo).to_json()
        except IndexError:
            return {}


class MonthlyMeasurements(Measurements):
    def __init__(self, device: dict):
        super().__init__(device, True)

    def requestLatestMeasurement(self, year: int, month: int):
        return super().requestLatestMeasurement(*requestMonthBounds(year, month))

    def requestOldestMeasurement(self, year: int, month: int):
        return super().requestOldestMeasurement(*requestMonthBounds(year, month))

    def requestMeasurementCount(self, year, month, additionalParameters: dict = None):
        dateFrom, dateTo = requestMonthBounds(year, month)
        return super().requestMeasurementCount(dateFrom, dateTo, additionalParameters)

    @staticmethod
    def fileName(year: int, month: int):
        dateFrom, dateTo = requestMonthBounds(year, month)
        return f'c8y_measurements ({dateFrom} - {dateTo}).json'


def requestMonthBounds(year: int, month: int):
    inclusiveDateFrom = date(year, month, 1)
    exclusiveDateTo = inclusiveDateFrom + relativedelta(months=1)
    return inclusiveDateFrom, exclusiveDateTo
