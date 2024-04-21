from datetime import date

from .config import getCumulocityApi
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta

c8y = getCumulocityApi()


class MonthlyMeasurements:

    def __init__(self, deviceObj: dict, year: int, month: int):
        dateFrom = date(year, month, 1)
        dateTo = dateFrom + relativedelta(months=1)

        if dateFrom > dateTo:
            raise ValueError("dateTo cannot be before dateFrom")

        self.deviceId = deviceObj['id']
        self.dateFrom = dateFrom  # inclusive
        self.dateTo = dateTo  # non-inclusive
        self.supportedFragmentAndSeries = deviceObj['c8y_supportedSeries']

        if deviceObj['latestMeasurement']:
            self.latestMeasurement = parse(deviceObj['latestMeasurement']['time']).date()
        if deviceObj['oldestMeasurement']:
            self.oldestMeasurement = parse(deviceObj['oldestMeasurement']['time']).date()

    def hasMeasurements(self):
        if not self.supportedFragmentAndSeries:
            return False
        if not self.latestMeasurement or not self.oldestMeasurement:
            return False
        if self.latestMeasurement < self.dateFrom or self.dateTo < self.oldestMeasurement:
            return False
        return True

    def requestLatestMeasurement(self):
        additionalParameters = {'revert': 'true'}
        return self.requestMeasurementCount(additionalParameters)

    def requestOldestMeasurement(self):
        additionalParameters = {'revert': 'false'}
        response = self.requestMeasurementCount(additionalParameters)
        return {'count': response['count'], 'oldestMeasurement': response['latestMeasurement']}

    def requestFragmentSeries(self):
        """
        return [{fragment, series, count, latestMeasurement}, ...]
        """
        fragmentSeries = []
        for seriesObj in self.supportedFragmentAndSeries:
            fragment = seriesObj['fragment']
            series = seriesObj['series']

            if self.hasMeasurements():
                additionalParameters = {'valueFragmentType': fragment, 'valueFragmentSeries': series}
                try:
                    measurementCount, latestMeasurement = self.requestMeasurementCount(additionalParameters)
                except:
                    measurementCount = -1
                    latestMeasurement = {}
            else:
                measurementCount = 0
                latestMeasurement = {}

            fragmentSeries.append({
                "fragment": fragment,
                "series": series,
                "count": measurementCount,
                'latestMeasurement': latestMeasurement
            })
        return fragmentSeries

    def requestMeasurementCount(self, additionalParameters: dict = None):
        parameters = {
            'dateFrom': self.dateFrom.isoformat(),
            'dateTo': self.dateTo.isoformat(),
            'source': self.deviceId,
            'pageSize': 1,
            'currentPage': 1,
            'withTotalPages': 'true',
            'revert': 'false',  # returns most recent measurement
        }
        if additionalParameters:
            parameters.update(additionalParameters)

        if self.hasMeasurements():
            response = c8y.get(resource="/measurement/measurements", params=parameters)
            measurementCount = response['statistics']['totalPages']
            latestMeasurement = response['measurements']
        else:
            measurementCount = 0
            latestMeasurement = {}

        if latestMeasurement:
            latestMeasurement = latestMeasurement[0]
            del latestMeasurement['self']
            del latestMeasurement['source']['self']

        return {'count': measurementCount, 'latestMeasurement': latestMeasurement}

    def requestLatestMeasurementValidation(self):
        try:
            return c8y.measurements.get_last(source=self.deviceId, before=self.dateTo).to_json()
        except IndexError:
            return {}


class TotalMeasurements(MonthlyMeasurements):
    def __init__(self, deviceObj: dict, exclusiveStartingDate: date, nonInclusiveDateTo: date):
        missingParameters = {'latestMeasurement': {}, 'oldestMeasurement': {}}
        super().__init__({**missingParameters, **deviceObj}, 1970, 1)

        # Fixed dates
        self.dateFrom = exclusiveStartingDate
        self.dateTo = nonInclusiveDateTo

    def hasMeasurements(self):
        if not self.supportedFragmentAndSeries:
            return False
        return True

    def requestFragmentSeries(self):
        raise Exception("don't call this from subclass")
