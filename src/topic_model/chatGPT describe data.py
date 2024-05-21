import pandas as pd
import instructor
import os
from skllm.config import SKLLMConfig
from tqdm import tqdm
from src.utils import readFile
from pydantic import BaseModel
from openai import OpenAI
from src.utils import tqdmFormat, getPath

SKLLMConfig.set_openai_key(os.getenv('OPENAI_API_KEY'))
SKLLMConfig.set_openai_org(os.getenv('OPENAPI_ORGANIZATION_ID'))

client = instructor.patch(OpenAI())
inputData = readFile('chatGPT input.json')
model = "gpt-4-turbo" # "gpt-3.5-turbo"


class Measurement(BaseModel):
    description: str
    domain: str
    subdomain: str


def requestChatGPTDescription(message_data, model):
    return client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content":
                    'Take on the persona of a data analyst who is proficient in interpreting JSON objects and '
                    'extracting meaningful insights from them. '
                    'The user will provide JSON objects representing measurement data from a smart city IoT device.'

            },
            {
                "role": "user",
                "content": 'Ignore device specific information and concisely summarise what kind of data is being sent. '
                           'Avoid generic terms like "IoT" and "smart city". '
                           'Also provide example of a smart city domain this device belongs to.'
                           f"{message_data} "
            }
        ],
        max_retries=5,
        response_model=Measurement
    )


result = {}
failedRequests = []
for deviceId, data in tqdm(inputData.items(), desc="requesting data for gpt-3.5-turbo", bar_format=tqdmFormat):
    try:
        response = requestChatGPTDescription(data, model)
        result[deviceId] = {
            'id': deviceId,
            'name': data['device'],
            'domain': response.domain,
            'description': response.description,
            'isAggregated': response.isAggregated,
            'input': data
        }
    except:
        failedRequests.append(deviceId)

df = pd.DataFrame(result.values())
df.to_csv(getPath(f"{model} description.csv"), encoding='utf-8-sig', index=False)
