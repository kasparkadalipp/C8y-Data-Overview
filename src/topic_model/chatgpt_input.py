from collections import defaultdict
from src.utils import saveToFile
from src.utils import listFileNames, readFile


def formatUnits(units):
    units = [unit for unit in units if unit is not None and unit != '']
    if len(units) == 1:
        return units[0]
    if len(units) == 0:
        return ''
    return units


def shortenEvent(event):
    def process_node(node, path=tuple()):
        if isinstance(node, dict):
            return {k: process_node(v, path + (k,)) for k, v in node.items()}
        elif isinstance(node, list):
            # Retain only the first element of the array
            if len(node) > 1:
                return [process_node(node[0], path)] if node else []
            return [process_node(node[0], path)] if node else []
        else:
            return node

    return process_node(event)


def createMeasurementMapping():
    measurementMapping = defaultdict(lambda: {'count': 0, 'units': set(), 'example': {}})
    for fileName in listFileNames('measurements/typeFragmentSeries/'):
        for device in readFile(fileName):
            deviceType = device['deviceType']
            deviceId = device['deviceId']

            for fragmentSeries in device['typeFragmentSeries']:
                measurementType = fragmentSeries['type']
                fragment = fragmentSeries['fragment']
                series = fragmentSeries['series']
                count = fragmentSeries['count']

                measurement = fragmentSeries['measurement']
                if measurement:
                    measurementValue = measurement[fragment][series]
                    unit = measurementValue['unit'] if 'unit' in measurementValue else ''
                    key = (deviceId, deviceType, measurementType, fragment, series)
                    measurementMapping[key]['units'].add(unit)
                    measurementMapping[key]['count'] += count
                    measurementMapping[key]['example'] = measurement
    return measurementMapping


def createEventMapping():
    eventMapping = {}
    for fileName in listFileNames('events/type/'):
        for event in readFile(fileName):
            deviceId = event['deviceId']
            deviceType = event['deviceType']
            for eventTypeObj in event['eventByType']:
                eventType = eventTypeObj['type']
                event = eventTypeObj['event']
                if event:
                    key = (deviceId, deviceType, eventType)
                    if key in eventMapping:
                        continue
                    eventMapping[key] = event
    return eventMapping


def createDatasetMapping():
    c8y_data = readFile('c8y_data.json')
    deviceIdMapping = {device['id']: device for device in c8y_data}
    measurementMapping = createMeasurementMapping()
    eventMapping = createEventMapping()

    deviceMapping = {}
    for key, value in measurementMapping.items():
        deviceId, deviceType, measurementType, fragment, series = key
        deviceName = deviceIdMapping[deviceId]['name']
        units = formatUnits(value['units'])

        if deviceId not in deviceMapping:
            deviceMapping[deviceId] = {'device': deviceName}

        device = deviceMapping[deviceId]
        if fragment not in device:
            device[fragment] = {}
        if series not in device[fragment]:
            device[fragment][series] = units

    for key, event in eventMapping.items():
        deviceId, deviceType, eventType = key
        deviceName = deviceIdMapping[deviceId]['name']
        if deviceId not in deviceMapping:
            deviceMapping[deviceId] = {'device': deviceName}

        alwaysPresentKeys = ["lastUpdated", "creationTime", "self", "source", "time", "id", "text", "type"]
        exampleEvent = {key: value for key, value in event.items() if key not in alwaysPresentKeys}

        deviceMapping[deviceId][eventType] = shortenEvent(exampleEvent)

    saveToFile(deviceMapping, "topic model/chatGPT input.json", overwrite=True)
