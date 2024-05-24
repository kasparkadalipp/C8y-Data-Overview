import calendar
from datetime import date
from dateutil.relativedelta import relativedelta
from src.cumulocity.requests.mapping.measurementTypeMapping import createMeasurementMapping
from src.cumulocity import MonthlyMeasurements, getCumulocityApi, requestMonthBounds
from src.utils import tqdmFormat, saveToFile, pathExists, readFile, ensureFileAndRead
from tqdm import tqdm


def requestTypeFragmentSeries(year, month):
    validation = {}
    for obj in readFile(f"measurements/fragmentSeries/{MonthlyMeasurements.fileName(year, month)}"):
        deviceId = obj['deviceId']
        validation[deviceId] = sum([measurement['count'] for measurement in obj['fragmentSeries']])

    c8y_data = readFile(f'c8y_data.json')
    typeFragmentSeriesMapping = ensureFileAndRead(f'measurements/c8y_measurements_id_to_type_mapping.json', createMeasurementMapping)
    c8y = getCumulocityApi()

    result = []
    updateTypes = False
    for device in tqdm(c8y_data, desc=f"{calendar.month_abbr[month]} {year}", bar_format=tqdmFormat):
        deviceId = device['id']
        count = 0

        c8y_measurements = {
            "deviceId": deviceId,
            "deviceType": device['type'],
            "typeFragmentSeries": []
        }
        knownTypes = set([tuple(_) for _ in typeFragmentSeriesMapping[deviceId]])
        typesToRequest = set([tuple(_) for _ in typeFragmentSeriesMapping[deviceId]])
        generator = None
        requestedMeasurements = 0
        while count < validation[deviceId]:
            if typesToRequest:
                measurementType, fragment, series = typesToRequest.pop()
            else:
                if generator is None:
                    dateFrom, dateTo = requestMonthBounds(year, month)
                    generator = c8y.measurements.select(source=deviceId, page_size=2000, after=dateFrom, before=dateTo)

                foundNewType = False
                for measurement in generator:
                    requestedMeasurements += 1
                    measurementType = measurement.type
                    for supported in device['c8y_supportedSeries']:
                        fragment = supported['fragment']
                        series = supported['series']
                        if fragment in measurement and series in measurement[fragment]:
                            key = (measurementType, fragment, series)
                            if key not in knownTypes:
                                typesToRequest.add(key)
                                knownTypes.add(key)
                                typeFragmentSeriesMapping[deviceId].append([measurementType, fragment, series])
                                foundNewType = True
                                updateTypes = True
                    if foundNewType:
                        break
                if foundNewType:
                    continue
                else:
                    print("No new types found")
                    break

            response = (MonthlyMeasurements(device, enforceBounds=True)
                        .requestTypeFragmentSeriesCount(year, month, measurementType, fragment, series))

            while response['count'] < 0:
                response = MonthlyMeasurements(device).requestAggregatedTypeFragmentSeriesCount(
                    year, month, measurementType, fragment, series)

            count += response['count']
            c8y_measurements['typeFragmentSeries'].append({
                "type": measurementType,
                "fragment": fragment,
                "series": series,
                "count": response['count'],
                "measurement": response['measurement']
            })
        result.append(c8y_measurements)
    if updateTypes:
        saveToFile(typeFragmentSeriesMapping, 'measurements/c8y_measurements_id_to_type_mapping.json')

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

        filePath = f"measurements/typeFragmentSeries/{MonthlyMeasurements.fileName(year, month)}"
        if pathExists(filePath):
            print(f"{calendar.month_abbr[month]} {year} - skipped")
        else:
            data = requestTypeFragmentSeries(year, month)
            saveToFile(data, filePath)

        currentDate -= relativedelta(months=1)
