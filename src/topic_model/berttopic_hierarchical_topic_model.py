import os
from bertopic import BERTopic
from bertopic.backend import BaseEmbedder
from bertopic.representation import OpenAI as BertOpenAI
from openai import OpenAI
from skllm.config import SKLLMConfig
from tqdm import tqdm
from src.utils import saveToFile, readFile, tqdmFormat, pathExists

SKLLMConfig.set_openai_key(os.getenv('OPENAI_API_KEY'))
SKLLMConfig.set_openai_org(os.getenv('OPENAPI_ORGANIZATION_ID'))


def requestEmbeddings(text, model="text-embedding-3-large"):
    client = OpenAI()
    return client.embeddings.create(input=text, model=model).data[0].embedding


def getDeviceEmbeddings(devices, model):
    fileName = f'{model} embeddings.json'
    deviceEmbeddings = readFile(fileName) if pathExists(fileName) else {}

    embeddings = []
    addedValues = {}
    for deviceId, device in tqdm(devices.items(), desc="getting embeddings", bar_format=tqdmFormat):
        if deviceId in deviceEmbeddings:
            embeddings.append(deviceEmbeddings[deviceId])
        else:
            textEmbedding = requestEmbeddings(device)
            embeddings.append(textEmbedding)
            addedValues[deviceId] = textEmbedding

    if len(addedValues) > 0:
        deviceEmbeddings.update(addedValues)
        saveToFile(deviceEmbeddings, fileName)
    return embeddings


def createTopicModel(model="gpt-4-turbo"):
    fileName = f"topic model/{model} descriptions.csv"
    deviceDescriptions = readFile(fileName)
    embeddings = getDeviceEmbeddings(deviceDescriptions, model)
    docs = list(deviceDescriptions.values())

    class CustomEmbedder(BaseEmbedder):
        def __init__(self, embedding_model):
            super().__init__()
            self.embedding_model = embedding_model

        def embed(self, documents, verbose=False):
            return embeddings

    prompt = """
    I have a topic that contains the following documents: 
    [DOCUMENTS]
    The topic is described by the following keywords: [KEYWORDS]
    
    Based on the information above, extract a short but highly descriptive topic label of at most 5 words. Make sure it is in the following format:
    topic: <topic label>
    """

    openai_model = BertOpenAI(OpenAI(), model=model, exponential_backoff=True, chat=True, prompt=prompt)
    topic_model = BERTopic(representation_model=openai_model, embedding_model=CustomEmbedder, nr_topics="auto")
    topics, probs = topic_model.fit_transform(docs)
    hierarchical_topics = topic_model.hierarchical_topics(docs)

    createTopicModelMapping(docs, topic_model, hierarchical_topics, topics)

    return topic_model, hierarchical_topics, topics, probs


def createTopicModelMapping(docs, topic_model, hierarchical_topics, topics):
    deviceMapping = {deviceId: str(topicId) for deviceId, topicId in zip(docs, topics)}
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
    saveToFile(nodeMapping, 'topic model/topic model.json', True)
