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

        if deviceObj['lastMeasurement']:
            self.lastMeasurement = parse(deviceObj['lastMeasurement']['time']).date()
        if deviceObj['firstMeasurement']:
            self.firstMeasurement = parse(deviceObj['firstMeasurement']['time']).date()

    def hasMeasurements(self):
        if not self.supportedFragmentAndSeries:
            return False
        if not self.lastMeasurement or not self.firstMeasurement:
            return False
        if self.lastMeasurement < self.dateFrom or self.dateTo < self.firstMeasurement:
            return False
        return True

    def requestLastMeasurement(self):
        additionalParameters = {'revert': 'true'}
        return self.requestMeasurementCount(additionalParameters)

    def requestFirstMeasurement(self):
        additionalParameters = {'revert': 'false'}
        response = self.requestMeasurementCount(additionalParameters)
        return {'count': response['count'], 'firstMeasurement': response['lastMeasurement']}

    def requestFragmentSeries(self):
        """
        return [{fragment, series, count, lastMeasurement}, ...]
        """
        fragmentSeries = []
        for seriesObj in self.supportedFragmentAndSeries:
            fragment = seriesObj['fragment']
            series = seriesObj['series']

            if self.hasMeasurements():
                additionalParameters = {'valueFragmentType': fragment, 'valueFragmentSeries': series}
                try:
                    measurementCount, lastMeasurement = self.requestMeasurementCount(additionalParameters)
                except:
                    measurementCount = -1
                    lastMeasurement = {}
            else:
                measurementCount = 0
                lastMeasurement = {}

            fragmentSeries.append({
                "fragment": fragment,
                "series": series,
                "count": measurementCount,
                'lastMeasurement': lastMeasurement
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
            lastMeasurement = response['measurements']
        else:
            measurementCount = 0
            lastMeasurement = {}

        if lastMeasurement:
            lastMeasurement = lastMeasurement[0]
            del lastMeasurement['self']
            del lastMeasurement['source']['self']

        return {'count': measurementCount, 'lastMeasurement': lastMeasurement}

    def requestLastMeasurementValidation(self):
        try:
            return c8y.measurements.get_last(source=self.deviceId).to_json()
        except IndexError:
            return {}


class TotalMeasurements(MonthlyMeasurements):
    def __init__(self, deviceObj: dict, exclusiveStartingDate: date, nonInclusiveDateTo: date):
        missingParameters = {'lastMeasurement': {}, 'firstMeasurement': {}}
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
