from datetime import date

from tqdm import tqdm

from .config import getCumulocityApi
from src.utils import tqdmFormat


# c8y_supportedSeries
def getSupportedMeasurements(devices: list) -> list:
    c8y = getCumulocityApi()

    data = []
    for deviceObj in tqdm(devices, desc="Requesting supported measurements", bar_format=tqdmFormat):
        device = deviceObj["device"]

        result = set()

        supportedFragments = c8y.inventory.get_supported_measurements(device['id'])  # fragment
        supportedSeries = c8y.inventory.get_supported_series(device['id'])  # fragment.series or just series

        for fragment in supportedFragments:
            for fullName in supportedSeries:
                if fragment == fullName:
                    result.add((fragment, fullName))

                elif fullName.startswith(fragment):
                    series = fullName[len(fragment):]
                    if series.startswith('.'):
                        series = series[1:]
                        result.add((fragment, series))
        data.append(
            {
                **deviceObj,
                'c8y_supportedSeries': [{'fragment': fragment, 'series': series} for fragment, series in result],
            }
        )
    return data


# lastMeasurement: }
# hasSentMeasurements: bool
def getLastMeasurement(devices):
    c8y = getCumulocityApi()

    def requestLastMeasurement(deviceId):
        try:
            return c8y.measurements.get_last(source=deviceId).to_json()
        except IndexError:
            return None

    data = []
    for deviceObj in tqdm(devices, desc="Requesting last measurement", bar_format=tqdmFormat):
        device = deviceObj["device"]

        if deviceObj['c8y_supportedSeries']:
            lastMeasurement = requestLastMeasurement(device['id'])
        else:
            lastMeasurement = None

        data.append(
            {
                **deviceObj,
                'lastMeasurement': lastMeasurement,
                'hasSentMeasurements': lastMeasurement is not None
            }
        )
    return data


# measurementCount: int
# hasMeasurements: bool
def getMeasurementCount(devices, dateFrom: date, dateTo: date):
    c8y = getCumulocityApi()

    data = []
    for deviceObj in tqdm(devices, desc="Requesting measurement count", bar_format=tqdmFormat):
        device = deviceObj['device']

        if not deviceObj['c8y_supportedSeries']:
            data.append({
                **deviceObj,
                'measurementCount': 0
            })
            continue

        parameters = {
            'dateFrom': dateFrom.isoformat(),
            'dateTo': dateTo.isoformat(),
            'source': device['id'],
            'pageSize': 1,
            'currentPage': 1,
            'withTotalPages': 'true',
        }

        response = c8y.get(resource="/measurement/measurements", params=parameters)
        measurementCount = response['statistics']['totalPages']

        data.append({
            **deviceObj,
            'measurementCount': measurementCount,
            'hasMeasurements': measurementCount > 0,
        })
    return data


# measurements: [{type, fragment, series, count}, ...]
def getMeasurementCountForSeries(devices, dateFrom: date, dateTo: date):
    c8y = getCumulocityApi()

    data = []
    for deviceObj in tqdm(devices, desc="Requesting measurement series", bar_format=tqdmFormat):
        device = deviceObj['device']
        deviceId = device['id']

        if deviceObj['measurementCount'] == 0:
            data.append({**deviceObj, 'measurements': []})
            continue

        parameters = {
            'dateFrom': dateFrom.isoformat(),
            'dateTo': dateTo.isoformat(),
            'source': deviceId,
            'pageSize': 1,
            'currentPage': 1,
            'withTotalPages': 'true',
        }

        expectedTotal = deviceObj['measurementCount']
        currentTotal = 0
        measurementTypes = set()
        measurementValueSet = set()
        fragmentSeries = []

        index = 0
        # Only way to get measurement type is to request actual data
        for measurement in c8y.measurements.select(source=deviceId, before=dateTo, after=dateFrom, page_size=2000):
            measurementType = measurement.type
            #  dataRequestLimit = 20_000
            # First request types used by actual measurement
            # If this fails, request every kind of fragment + series for type
            # if index >= dataRequestLimit:
            #     if measurementType in measurementTypes:
            #         continue
            #     else:
            #         measurementTypes.add(measurementType)

            for seriesObj in deviceObj['c8y_supportedSeries']:
                fragment = seriesObj['fragment']
                series = seriesObj['series']

                if (measurementType, fragment, series) in measurementValueSet:
                    continue
                # if index < dataRequestLimit and not (fragment in measurement and series in measurement[fragment]):
                if not (fragment in measurement and series in measurement[fragment]):
                    continue

                measurementValueSet.add((measurementType, fragment, series))
                parameters = {**parameters, 'type': measurementType, 'valueFragmentType': fragment,
                              'valueFragmentSeries': series}

                response = c8y.get(resource="/measurement/measurements", params=parameters)
                count = response['statistics']['totalPages']
                fragmentSeries.append({
                    "type": measurementType,
                    "fragment": fragment,
                    "series": series,
                    "count": count
                })
                currentTotal += count
            if currentTotal >= expectedTotal:
                break
            index += 1

        for measurementType, _, _ in measurementValueSet:
            measurementTypes.add(measurementType)

        data.append({
            **deviceObj,
            'measurements': fragmentSeries,
            'c8y_supportedMeasurementType': list(measurementTypes),
        })
    return data


def getUnitsAndMinMaxValues(devices, dateFrom: date, dateTo: date):
    c8y = getCumulocityApi()

    def combineResponseData(response):
        result = response['series']
        for values in response['values'].values():
            for index, value in enumerate(values):
                if value is None:
                    continue
                for key in ['max', 'min']:
                    if key in result[index]:
                        result[index][key] = max(result[index][key], value[key])
                    else:
                        result[index][key] = value[key]
        return result

    data = []
    for deviceObj in tqdm(devices, desc="Requesting daily aggregated data", bar_format=tqdmFormat):
        device = deviceObj['device']

        if not deviceObj['c8y_supportedSeries']:
            data.append({
                **deviceObj,
                'measurements': []
            })
            continue

        response = c8y.measurements.get_series(
            aggregation="DAILY",
            after=dateFrom.isoformat(),
            before=dateTo.isoformat(),
            source=device['id'])

        result = combineResponseData(response)
        for item in result:
            fragment = item['type']
            series = item['name']
            del item['type']
            del item['name']
            item['fragment'] = fragment
            item['series'] = series

        data.append({
            **deviceObj,
            'measurementsMinMax': result
        })
    return data
