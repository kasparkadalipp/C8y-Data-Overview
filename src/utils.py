import json
from pathlib import Path

tqdmFormat = "{l_bar}{bar}| {n_fmt}/{total_fmt} [time elapsed: {elapsed}]"

dataRoot = "../data/"


def saveToFile(devices: list, filePath: str, overwrite: bool):
    path = Path(f"{dataRoot}{filePath}")

    if not overwrite and path.exists():
        return 'skipped'

    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w+", encoding='utf8') as file:
        json.dump(devices, file, indent=2, ensure_ascii=False)


def pathExists(filePath: str):
    path = Path(f"{dataRoot}{filePath}")
    return path.exists()
