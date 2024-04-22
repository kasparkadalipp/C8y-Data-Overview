import pytest
import json
import os

basePath = "../data/telia/events/"


def getFiles(folder):
    return os.listdir(basePath + folder)


@pytest.mark.parametrize("fileName", getFiles('total'))
class TestTotalMeasurements:
    folder = 'total'
    example = {
        "deviceId": 11904,
        "deviceType": "com_cityntel_light",
        "total": {
            "count": 15660,
            "event": "<measurement>"
        }
    }

    def getContents(self, fileName):
        filePath = f"{basePath}{self.folder}/{fileName}"
        with open(filePath, 'r', encoding='utf8') as json_file:
            return json.load(json_file)

    def test_required_keys(self, fileName):
        for device in self.getContents(fileName):
            for key in self.example.keys():
                assert key in device.keys()
            for key in self.example['total'].keys():
                assert key in device['total'].keys()

    def test_no_failed_requests(self, fileName):
        for device in self.getContents(fileName):
            if device['total']['count'] == -2:  # Ignored Event
                continue
            assert device['total']['count'] >= 0
