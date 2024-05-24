import calendar
from datetime import date
from dateutil.relativedelta import relativedelta
from src.cumulocity import MonthlyMeasurements
from src.utils import tqdmFormat, saveToFile, pathExists, readFile
from tqdm import tqdm


def requestFragmentSeries(year, month):
    c8y_data = readFile(f'c8y_data.json')
    result = []
    for device in tqdm(c8y_data, desc=f"{calendar.month_abbr[month]} {year}",
                       bar_format=tqdmFormat):

        c8y_measurements = {
            "deviceId": device['id'],
            "deviceType": device['type'],
            "fragmentSeries": []
        }

        for fragmentSeries in device['c8y_supportedSeries']:
            fragment = fragmentSeries['fragment']
            series = fragmentSeries['series']

            response = MonthlyMeasurements(device, enforceBounds=True).requestFragmentSeriesCount(year, month, fragment, series)
            while response['count'] < 0:
                response = MonthlyMeasurements(device, enforceBounds=True).requestAggregatedFragmentSeriesCount(year, month, fragment, series)

            c8y_measurements['fragmentSeries'].append({
                "fragment": fragment,
                "series": series,
                "count": response['count'],
                "measurement": response['measurement']
            })
        result.append(c8y_measurements)
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

        filePath = f"measurements/fragmentSeries/{MonthlyMeasurements.fileName(year, month)}"
        if pathExists(filePath):
            print(f"{calendar.month_abbr[month]} {year} - skipped")
        else:
            data = requestFragmentSeries(year, month)
            saveToFile(data, filePath)

        currentDate -= relativedelta(months=1)
