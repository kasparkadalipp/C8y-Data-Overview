import pandas as pd
from genson import SchemaBuilder
from collections import defaultdict
from src.utils import listFileNames, readFile, getPath, mapToJsonSchema


def createEventTypeMapping():
    eventTypeMapping = defaultdict(
        lambda: {'schema': SchemaBuilder(schema_uri=False), 'count': 0, 'example': {}, 'devices': set()})
    for fileName in listFileNames('events/type/'):
        for device in readFile(fileName):
            deviceId = device['deviceId']
            deviceType = device['deviceType']
            for event in device['eventByType']:
                eventType = event['type']
                device = event['event']
                count = event['count']
                if device:
                    jsonSchema = mapToJsonSchema(device)
                    key = (deviceType, eventType)
                    eventTypeMapping[key]['schema'].add_schema(jsonSchema)
                    eventTypeMapping[key]['count'] += count
                    eventTypeMapping[key]['example'] = device
                    eventTypeMapping[key]['devices'].add(deviceId)
    return eventTypeMapping


def createEventTypeFragmentMapping():
    eventTypeFragmentMapping = defaultdict(
        lambda: {'schema': SchemaBuilder(schema_uri=False), 'count': 0, 'example': {}, 'devices': set()})
    for fileName in listFileNames('events/typeFragment/'):
        for device in readFile(fileName):
            eventTypeSum = 0
            deviceId = device['deviceId']
            deviceType = device['deviceType']
            for event in device['typeFragment']:
                eventType = event['type']
                device = event['event']
                count = event['count']
                fragment = event['fragment']
                if device:
                    jsonSchema = mapToJsonSchema(device)
                    key = (deviceType, eventType, fragment)
                    eventTypeFragmentMapping[key]['schema'].add_schema(jsonSchema)
                    eventTypeFragmentMapping[key]['count'] += count
                    eventTypeFragmentMapping[key]['example'] = device
                    eventTypeFragmentMapping[key]['devices'].add(deviceId)
    return eventTypeFragmentMapping


def createEventTypeFragmentSchema():
    eventTypeMapping = createEventTypeMapping()
    eventTypeFragmentMapping = createEventTypeFragmentMapping()
    data = []
    usedEventTypes = set()
    sortedTypeFragment = dict(sorted(eventTypeFragmentMapping.items(), reverse=True,
                                     key=lambda item: (item[0][0], item[0][1], item[1]['count'])))
    for eventTypeFragment, typeFragment in sortedTypeFragment.items():
        eventType, deviceType, fragment = eventTypeFragment

        eventTypeKey = (eventType, deviceType)
        eventTypeCount = eventTypeMapping[eventTypeKey]['count']
        deviceCountForType = len(eventTypeMapping[eventTypeKey]['devices'])
        deviceCountForFragment = len(typeFragment['devices'])

        if eventTypeKey in usedEventTypes:
            deviceType = ''
            eventType = ''
            eventTypeCount = ''
            deviceCountForType = ''
        else:
            usedEventTypes.add(eventTypeKey)

        row = {
            'devicesCount': deviceCountForType,
            'deviceType': deviceType,
            'eventType': eventType,
            'eventCount': eventTypeCount,
            'fragmentDeviceCount': deviceCountForFragment,
            'fragment': fragment,
            'fragmentCount': typeFragment['count'],
            'jsonSchema': str(typeFragment['schema'].to_schema()).replace("'", '"'),
            'example event': typeFragment['example']
        }
        data.append(row)
    for key, values in eventTypeMapping.items():
        if key not in usedEventTypes:
            deviceType, eventType = key
            row = {
                'devicesCount': len(values['devices']),
                'deviceType': deviceType,
                'eventType': eventType,
                'count': values['count'],
                'fragment': '',
                'fragmentCount': '',
                'jsonSchema': str(values['schema'].to_schema()).replace("'", '"'),
                'example event': values['example']
            }
            data.append(row)
    df = pd.DataFrame(data)
    df.to_csv(getPath('Events (type + fragment).csv'), index=False, encoding='utf-8-sig')


def createEventTypeSchema():
    eventTypeMapping = createEventTypeMapping()

    data = []
    for key, values in eventTypeMapping.items():
        deviceType, eventType = key
        row = {
            'deviceType': deviceType,
            'eventType': eventType,
            'count': values['count'],
            'jsonSchema': str(values['schema'].to_schema()).replace("'", '"'),
            'example event': values['example']
        }
        data.append(row)

    df = pd.DataFrame(data)
    df.to_csv(getPath('telia/Events (type).csv'), index=False, encoding='utf-8-sig')
