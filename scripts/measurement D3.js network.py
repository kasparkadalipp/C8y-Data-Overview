from collections import defaultdict

from src.utils import saveToFile, readFile, fileNamesInFolder
import re

basePath = '../data/telia/measurements/typeFragmentSeries/'


def fixSensorFragment(name):
    match = re.match("^(sensor)_\\d{1,4}(.*)", name)  # sensor_1235_daily -> sensor_daily
    if match:
        return match.group(1) + match.group(2)
    return name


def getUniqueFields(filePaths):
    result = defaultdict(int)

    for filePath in filePaths:
        for device in readFile(filePath):
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
    if group == 0:
        return "Measurements"
    return str(item)[2:]


def createNetworkData(inputData: dict):
    data = {"nodes": [], 'links': []}

    links = set()
    nodes = defaultdict(lambda: {'devices': set(), 'measurements': 0})

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
            (level1, 1),
            (level2, 2),
            (level3, 3),
            (level4, 4),
            (level5, 5)
        ]

        for key in keys:
            nodes[key]['measurements'] += count
            nodes[key]['devices'].add(deviceId)

        links.add((root, source1))
        links.add((source1, source2))
        links.add((source2, source3))
        links.add((source3, source4))
        links.add((source4, source5))

    data['links'] = [{"source": source, "target": target} for source, target in links]
    for node, count in nodes.items():
        nodeId, group = node
        data['nodes'].append(
            {
                "id": nodeId,
                "group": group,
                "name": formatName(nodeId, group),
                'measurementCount': count['measurements'],
                'deviceCount': len(count['devices'])
            })
    return data


def visualizeWholeDataset():
    inputData = getUniqueFields(fileNamesInFolder(basePath))
    network = createNetworkData(inputData)
    saveToFile(network, 'telia/visualizations/network (total).json', overwrite=True)


def visualiseMonth():
    inputData = getUniqueFields([basePath + 'c8y_measurements 2024-03-01 - 2024-04-01.json'])
    network = createNetworkData(inputData)
    saveToFile(network, 'telia/visualizations/network (month).json', overwrite=True)
