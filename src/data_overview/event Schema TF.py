import pandas as pd
from genson import SchemaBuilder
from collections import defaultdict
from src.utils import listFileNames, readFile, getPath, mapToJsonSchema

eventTypeFragmentMapping = defaultdict(lambda: {'schema': SchemaBuilder(schema_uri=False), 'count': 0, 'example': {}})

for fileName in listFileNames("events/typeFragment/"):
    for event in readFile(fileName):
        eventTypeSum = 0
        deviceId = event['deviceId']
        deviceType = event['deviceType']
        for eventTypeObj in event['typeFragment']:
            eventType = eventTypeObj['type']
            event = eventTypeObj['event']
            count = eventTypeObj['count']
            fragment = eventTypeObj['fragment']
            if event:
                jsonSchema = mapToJsonSchema(event)
                key = (deviceType, eventType, fragment)
                eventTypeFragmentMapping[key]['schema'].add_schema(jsonSchema)
                eventTypeFragmentMapping[key]['count'] += count
                eventTypeFragmentMapping[key]['example'] = event

data = []
for key, values in eventTypeFragmentMapping.items():
    deviceType, eventType, fragment = key
    row = {
        'deviceType': deviceType,
        'eventType': eventType,
        'fragment': fragment,
        'count': values['count'],
        'jsonSchema': str(values['schema'].to_schema()).replace("'", '"'),
        'example event': values['example']
    }
    data.append(row)

df = pd.DataFrame(data)
df.to_csv(getPath("telia/Events (type + fragment).csv"), index=False, encoding='utf-8-sig')
