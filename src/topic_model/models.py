from enum import Enum


class GPTModel(Enum):
    GPT_3_5_TURBO = "gpt-3.5-turbo"
    GPT_4_TURBO = "gpt-4-turbo"


class GPTEmbeddings(Enum):
    EMBEDDINGS_SMALL = "text-embedding-3-small"
    EMBEDDINGS_LARGE = "text-embedding-3-large"
