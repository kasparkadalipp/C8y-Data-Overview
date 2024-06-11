import re
from collections import defaultdict
from src.cumulocity import MonthlyMeasurements
from src.utils import saveToFile, readFile, listFileNames


def calculateFrequency(measurementCount: int, daysInMonth: int = 31):
    # Total seconds, minutes, hours, and days in the given month
    total_seconds = daysInMonth * 24 * 60 * 60
    total_minutes = daysInMonth * 24 * 60
    total_hours = daysInMonth * 24
    total_days = daysInMonth

    # Calculate frequencies
    frequency_per_second = measurementCount / total_seconds
    frequency_per_minute = measurementCount / total_minutes
    frequency_per_hour = measurementCount / total_hours
    frequency_per_day = measurementCount / total_days

    # Determine the most fitting unit
    if frequency_per_second >= 1:
        most_fitting = round(frequency_per_second, 2)
        unit = "second"
    elif frequency_per_minute >= 1:
        most_fitting = round(frequency_per_minute, 2)
        unit = "minute"
    elif frequency_per_hour >= 1:
        most_fitting = round(frequency_per_hour, 2)
        unit = "hour"
    else:
        most_fitting = round(frequency_per_day, 2)
        unit = "day"

    most_fitting_str = f"{most_fitting:.2f}".rstrip('0').rstrip('.')

    return f"{most_fitting_str}/{unit}"


def fixSensorFragment(name: str):
    match = re.match("^(sensor)_\\d{1,4}(.*)", name)  # sensor_1235_daily -> sensor_daily
    if match:
        return match.group(1) + match.group(2)
    return name


def getUniqueFields(filePaths: list):
    result = defaultdict(int)
    for fileName in filePaths:
        for device in readFile(fileName):
            deviceId = device['deviceId']
            deviceType = device['deviceType']

            for fragmentSeries in device['typeFragmentSeries']:
                measurementType = fragmentSeries['type']
                fragment = fragmentSeries['fragment']
                series = fragmentSeries['series']
                count = fragmentSeries['count']

                measurement = fragmentSeries['measurement']
                if measurement:
                    measurementValue = measurement[fragment][series]
                    unit = measurementValue['unit'] if 'unit' in measurementValue else ''
                    # value = str(measurementValue['value']) if 'value' in measurementValue else ''
                    result[(deviceId, deviceType, measurementType, fragment, series, unit)] += count
    return result


def formatName(item, group):
    return str(item)[2:]


def calculateTopPercentages(data, total_count):
    sorted_data = dict(sorted(data.items(), key=lambda item: item[1], reverse=True))
    top_entries = dict(list(sorted_data.items())[:5])  # top 5 entries
    percentages = [(k, f"{round((v / total_count) * 100, 2)}%") for k, v in top_entries.items()]
    return percentages


def createMeasurementNetwork(inputData: dict, addTopics=False):
    data = {"nodes": [], 'links': []}

    links = set()
    nodes = defaultdict(lambda: {'devices': set(), 'measurements': 0, 'topics': defaultdict(int)})

    if addTopics:
        topicModel = readFile('topic model/topic model.json')
    for key, count in inputData.items():
        deviceId, deviceType, measurementType, fragment, series, unit = key
        level1 = deviceType
        level2 = measurementType
        level3 = fragment
        level4 = series
        level5 = unit

        if level1 is None or level1 == "": level1 = "<missing deviceType>"
        if level2 is None or level2 == "": level2 = "<missing measurementType>"
        if level3 is None or level3 == "": level3 = "<missing fragment>"
        if level4 is None or level4 == "": level4 = "<missing series>"
        if level5 is None or level5 == "": level5 = "<missing unit>"

        root = "0_Measurements"
        source1 = f"1_{level1}"
        source2 = f"2_{level2}"
        source3 = f"3_{level3}"
        source4 = f"4_{level4}"
        source5 = f"5_{level5}"

        keys = [
            (root, 0),
            (source1, 1),
            (source2, 2),
            (source3, 3),
            (source4, 4),
            (source5, 5)
        ]

        links.add((root, source1))
        links.add((source1, source2))
        links.add((source2, source3))
        links.add((source3, source4))
        links.add((source4, source5))

        if addTopics:
            topic = topicModel['topic'][deviceId]
            topicName = topicModel['name'][topic]
            topicId = "T_" + topicName
            parent = topicModel['parent'][topic]
            parentId = "P_" + parent

            if not topicName == "Outlier Topic":
                nodes[(topicId, 6)]['measurements'] += count
                nodes[(topicId, 6)]['devices'].add(deviceId)

                if level5 == "<missing unit>":
                    links.add((source4, topicId))
                else:
                    links.add((source5, topicId))

        for key in keys:
            nodes[key]['measurements'] += count
            nodes[key]['devices'].add(deviceId)
            if addTopics:
                nodes[key]['topics'][topicName] += 1

    data['links'] = [{"source": source, "target": target} for source, target in links]

    for node, count in nodes.items():
        nodeId, group = node
        data['nodes'].append(
            {
                "id": nodeId,
                "group": group,
                "name": formatName(nodeId, group),
                "frequency": calculateFrequency(count['measurements']),
                "topics": calculateTopPercentages(count['topics'], len(count['devices'])),
                'dataCount': count['measurements'],
                'deviceCount': len(count['devices'])
            })
    return data


def visualizeWholeDataset():
    inputData = getUniqueFields(listFileNames('measurements/typeFragmentSeries/'))
    network = createMeasurementNetwork(inputData)
    saveToFile(network, 'visualizations/network (total).json')


def visualizeMonth(year: int, month: int):
    fileName = MonthlyMeasurements.fileName(year, month)
    inputData = getUniqueFields([f'measurements/typeFragmentSeries/{fileName}'])
    network = createMeasurementNetwork(inputData)
    saveToFile(network, 'visualizations/network (month).json')
