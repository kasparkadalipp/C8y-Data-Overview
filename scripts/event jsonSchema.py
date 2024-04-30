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



folder = "telia/events/type/"

eventTypeMapping = defaultdict(lambda: {'schema': SchemaBuilder(schema_uri=False), 'count': 0, 'example': {}})

for fileName in fileNamesInFolder('../data/' + folder):
    for event in readFile(folder + fileName):
        eventTypeSum = 0
        deviceId = event['deviceId']
        for eventTypeObj in event['eventByType']:
            eventType = eventTypeObj['type']
            event = eventTypeObj['event']
            count = eventTypeObj['count']
            if event:
                jsonSchema = createSchema(event)
                eventTypeMapping[eventType]['schema'].add_schema(jsonSchema)
                eventTypeMapping[eventType]['count'] += count
                eventTypeMapping[eventType]['example'] = event

data = []
for eventType, values in eventTypeMapping.items():
    row = {'eventType': eventType, 'count': values['count'],
           'jsonSchema': str(values['schema'].to_schema()).replace("'", '"'), 'example event': values['example']}
    data.append(row)

df = pd.DataFrame(data)
df.to_csv("../data/telia/Event jsonSchema.csv", index=False)