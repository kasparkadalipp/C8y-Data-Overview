import os
from bertopic import BERTopic
from bertopic.backend import BaseEmbedder
from bertopic.representation import OpenAI as BertOpenAI
from openai import OpenAI
from skllm.config import SKLLMConfig
from src.topic_model.embeddings import getDeviceEmbeddings
from src.topic_model.models import GPTModel
from src.utils import saveToFile, readFile

SKLLMConfig.set_openai_key(os.getenv('OPENAI_API_KEY'))
SKLLMConfig.set_openai_org(os.getenv('OPENAPI_ORGANIZATION_ID'))


def createTopicModel(gptModel: GPTModel = GPTModel.GPT_4_TURBO):
    model = gptModel.value
    fileName = f"topic model/{model} descriptions.json"
    deviceDescriptions = readFile(fileName)
    embeddings = getDeviceEmbeddings(gptModel)
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
    topic_model = BERTopic(representation_model={"OpenAI": openai_model}, embedding_model=CustomEmbedder, nr_topics='auto')
    topics, probs = topic_model.fit_transform(docs)
    hierarchical_topics = topic_model.hierarchical_topics(docs)

    chatgpt_topic_labels = {topic: " | ".join(list(zip(*values))[0]) for topic, values in topic_model.topic_aspects_["OpenAI"].items()}
    chatgpt_topic_labels[-1] = "Outlier Topic"

    createTopicModelMapping(docs, hierarchical_topics, topics, chatgpt_topic_labels)

    return topic_model, hierarchical_topics, topics, probs, chatgpt_topic_labels


def createTopicModelMapping(docs, hierarchical_topics, topics, chatgpt_topic_labels):
    deviceMapping = {deviceId: str(topicId) for deviceId, topicId in zip(docs, topics)}
    saveToFile(deviceMapping, 'visualisations/deviceID topicID mapping.json', True)

    nameMapping = {}
    parentMapping = {}
    rootNode = -1
    for _, row in hierarchical_topics.iterrows():
        parent = int(row['Parent_ID'])
        leftChild = int(row['Child_Left_ID'])
        rightChild = int(row['Child_Right_ID'])
        rootNode = max(rootNode, parent)
        # nodeTopics = row['Topics']

        nameMapping[parent] = chatgpt_topic_labels.get(parent, 'default')
        parentMapping[leftChild] = str(parent)
        parentMapping[rightChild] = str(parent)
    for topicId, label in chatgpt_topic_labels.items():
        nameMapping[topicId] = chatgpt_topic_labels[topicId]
    rootNode = str(rootNode)
    parentMapping["-1"] = rootNode
    nameMapping[rootNode] = "Topic model root"
    nodeMapping = {'root': rootNode, 'name': nameMapping, 'parent': parentMapping}
    saveToFile(nodeMapping, 'topic model/topic model.json')
