import os
from skllm.config import SKLLMConfig

SKLLMConfig.set_openai_key(os.getenv('OPENAI_API_KEY'))
SKLLMConfig.set_openai_org(os.getenv('OPENAPI_ORGANIZATION_ID'))
from src.utils import saveToFile, readFile
from bertopic.representation import OpenAI as BertOpenAI
from bertopic import BERTopic

inputData = readFile('chatGPT input.json')

client = BertOpenAI()

BertTopicPrompt = """
I have a topic that contains the following documents: 
[DOCUMENTS]
The topic is described by the following keywords: [KEYWORDS]

Based on the information above, extract a short but highly descriptive topic label of at most 5 words. Make sure it is in the following format:
topic: <topic label>
"""

openai_model = BertOpenAI(client, model="gpt-4-turbo", exponential_backoff=True, chat=True, prompt=BertTopicPrompt)

docs = [str(device) for device in inputData.values()]
topic_model = BERTopic(representation_model={"OpenAI": openai_model}, nr_topics="auto")
topics, probs = topic_model.fit_transform(docs)
hierarchical_topics = topic_model.hierarchical_topics(docs)

deviceMapping = {deviceId: str(topicId) for deviceId, topicId in zip(inputData.keys(), topics)}
saveToFile(deviceMapping, 'visualisations/deviceID topicID mapping.json', True)

labelMapping = {topic: " | ".join(list(zip(*values))[0]) for topic, values in
                topic_model.topic_aspects_["OpenAI"].items()}
nameMapping = {}
parentMapping = {}

rootNode = -1
for _, row in hierarchical_topics.iterrows():
    parent = int(row['Parent_ID'])
    leftChild = int(row['Child_Left_ID'])
    rightChild = int(row['Child_Right_ID'])
    rootNode = max(rootNode, parent)
    # nodeTopics = row['Topics']

    nameMapping[parent] = labelMapping.get(parent, 'default')
    parentMapping[leftChild] = str(parent)
    parentMapping[rightChild] = str(parent)

for topicId, label in labelMapping.items():
    nameMapping[topicId] = labelMapping[topicId]

rootNode = str(rootNode)
parentMapping["-1"] = rootNode
nameMapping[rootNode] = "Topic model root"

nodeMapping = {'root': rootNode, 'name': nameMapping, 'parent': parentMapping}
saveToFile(nodeMapping, 'visualisations/topic model.json', True)
