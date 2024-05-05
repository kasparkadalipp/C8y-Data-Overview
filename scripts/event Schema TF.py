import pandas as pd
from genson import SchemaBuilder
from collections import defaultdict
from src.utils import fileNamesInFolder, readFile


def createSchema(event):
    alwaysPresentKeys = ["lastUpdated", "creationTime", "self", "source", "time", "id", "text", "type"]
    builder = SchemaBuilder(schema_uri=False)
    if isinstance(event, list):
        for item in event:
            builder.add_object({key: value for key, value in item.items() if key not in alwaysPresentKeys})
    else:
        builder.add_object({key: value for key, value in event.items() if key not in alwaysPresentKeys})
    jsonSchema = builder.to_schema()
    return jsonSchema


folder = "telia/events/typeFragment/"

eventTypeMapping = defaultdict(lambda: {'schema': SchemaBuilder(schema_uri=False), 'count': 0, 'example': {}})

for fileName in fileNamesInFolder('../data/' + folder):
    for event in readFile(folder + fileName):
        eventTypeSum = 0
        deviceId = event['deviceId']
        deviceType = event['deviceType']
        for eventTypeObj in event['typeFragment']:
            eventType = eventTypeObj['type']
            event = eventTypeObj['event']
            count = eventTypeObj['count']
            fragment = eventTypeObj['fragment']
            if event:
                jsonSchema = createSchema(event)
                key = (deviceType, eventType, fragment)
                eventTypeMapping[key]['schema'].add_schema(jsonSchema)
                eventTypeMapping[key]['count'] += count
                eventTypeMapping[key]['example'] = event

data = []
for key, values in eventTypeMapping.items():
    eventType, deviceType, fragment = key
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
df.to_csv("../data/telia/Events (type + fragment).csv", index=False, encoding='utf-8-sig')
