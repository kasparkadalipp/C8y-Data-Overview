from src.utils import saveToFile, readFile, fileNamesInFolder
import re

filtered = False


def formatName(item, group):
    if group == 0:
        return "Measurements"
    value = str(item)
    if "." in value:
        value = value.rsplit(".")[1]  # splits series
    return value[2:]


def fixSensorFragment(name):
    match = re.match("^(sensor)_\\d{1,4}(.*)", name)  # sensor_1235_daily -> sensor_daily
    if match:
        return match.group(1) + match.group(2)
    return name


folder = "telia/measurements/typeFragmentSeries/"

result = set()
fileList = fileNamesInFolder('../data/' + folder)
for fileName in [fileList[0]]:
    for device in readFile(folder + fileName):
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
                result.add((deviceId, deviceType, measurementType, fragment, series, unit))

links = set()
nodes = set()

for deviceId, deviceType, measurementType, fragment, series, unit in result:
    level1 = deviceType
    level2 = measurementType
    level3 = fixSensorFragment(fragment) if filtered else fragment
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

    nodes.add((root, 0))
    nodes.add((f"1_{level1}", 1))
    nodes.add((f"2_{level2}", 2))
    nodes.add((f"3_{level3}", 3))
    nodes.add((f"4_{level4}", 4))
    nodes.add((f"5_{level5}", 5))

    links.add((root, source1))
    links.add((source1, source2))
    links.add((source2, source3))
    links.add((source3, source4))
    links.add((source4, source5))

data = {
    "nodes": [{"id": id, "group": group, "name": formatName(id, group)} for id, group in nodes],
    "links": [{"source": source, "target": target} for source, target in links]
}

saveToFile(data, "c8y_measurements (test).json", overwrite=True)
