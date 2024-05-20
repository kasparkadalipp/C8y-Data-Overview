from dotenv import load_dotenv
import pandas as pd
import tiktoken
import os
import random
from dotenv import load_dotenv
from tqdm import tqdm
import json
from sklearn.model_selection import train_test_split
from skllm.models.gpt.classification.zero_shot import ZeroShotGPTClassifier
from skllm.models.gpt.classification.few_shot import FewShotGPTClassifier
from skllm.models.gpt.classification.few_shot import DynamicFewShotGPTClassifier
from skllm.models.gpt.vectorization import GPTVectorizer
from skllm.config import SKLLMConfig
from sklearn.manifold import TSNE
from sklearn.preprocessing import LabelEncoder
import matplotlib.pyplot as plt
SKLLMConfig.set_openai_key(os.getenv('OPENAI_API_KEY'))
SKLLMConfig.set_openai_org(os.getenv('OPENAPI_ORGANIZATION_ID'))
from tqdm import tqdm
import json
import random
from tqdm import tqdm
import yaml
from collections import defaultdict
from typing import List, Dict
from src.utils import saveToFile, readFile, listFileNames
from pydantic import BaseModel, Field
import instructor
import numpy as np
from bertopic.representation import KeyBERTInspired, MaximalMarginalRelevance, OpenAI, PartOfSpeech
from bertopic.representation import OpenAI as BertOpenAI
from scipy.cluster import hierarchy as sch
from bertopic import BERTopic
from bertopic.backend import BaseEmbedder
from pydantic import BaseModel, Field
import instructor

load_dotenv('../.env')
inputData = readFile('chatGPT input.json')


client = BertOpenAI()

BertTopicPrompt = """
I have a topic that contains the following documents: 
[DOCUMENTS]
The topic is described by the following keywords: [KEYWORDS]

Based on the information above, extract a short but highly descriptive topic label of at most 5 words. Make sure it is in the following format:
topic: <topic label>
"""

# model="gpt-4-turbo",
openai_model = BertOpenAI(client, model="gpt-3.5-turbo", exponential_backoff=True, chat=True, prompt=BertTopicPrompt)
representation_model = {"OpenAI": openai_model}

docs = [str(device) for device in inputData.values()]
topic_model = BERTopic(representation_model=representation_model, nr_topics="auto")
topics, probs = topic_model.fit_transform(docs)
hierarchical_topics = topic_model.hierarchical_topics(docs)




deviceMapping = {deviceId: str(topicId) for deviceId, topicId in zip(inputData.keys(), topics)}
saveToFile(deviceMapping, 'visualisations/deviceID topicID mapping.json', True)



labelMapping = {topic: " | ".join(list(zip(*values))[0]) for topic, values in topic_model.topic_aspects_["OpenAI"].items()}
nameMapping = {}
parentMapping = {}

rootNode = -1
for _, row in hierarchical_topics.iterrows():
    parent = int(row['Parent_ID'])
    leftChild = int(row['Child_Left_ID'])
    rightChild = int(row['Child_Right_ID'])
    rootNode = max(rootNode, parent)
    #nodeTopics = row['Topics']  # TODO describe topics

    nameMapping[parent] = labelMapping.get(parent, 'default')
    parentMapping[leftChild] = str(parent)
    parentMapping[rightChild] = str(parent)

for topicId, label in labelMapping.items():
    nameMapping[topicId] = labelMapping[topicId]

rootNode = str(rootNode)
parentMapping["-1"] = rootNode
nameMapping[rootNode] = "Topic model root"

nodeMapping = {'root': rootNode, 'name': nameMapping, 'parent': parentMapping}
saveToFile(nodeMapping,'visualisations/topic model.json',True)


