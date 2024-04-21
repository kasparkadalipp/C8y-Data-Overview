import pytest
import json
import os

basePath = "../data/telia/measurements/"


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
            "measurement": "<measurement>"
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
            assert device['total']['count'] >= 0


@pytest.mark.parametrize("fileName", getFiles('fragmentSeries'))
class TestFragmentSeries:
    folder = 'fragmentSeries'
    example = {
        "deviceId": 11904,
        "deviceType": "com_cityntel_light",
        "fragmentSeries": [
            {
                "fragment": "active_power",
                "series": "L1",
                "count": 2304,
                "measurement": "<measurement>"
            }
        ]
    }

    def getContents(self, fileName):
        filePath = f"{basePath}{self.folder}/{fileName}"
        with open(filePath, 'r', encoding='utf8') as json_file:
            return json.load(json_file)

    def test_required_keys(self, fileName):
        for device in self.getContents(fileName):
            for key in self.example.keys():
                assert key in device.keys()

            for measurement in device['fragmentSeries']:
                for key in self.example['fragmentSeries'][0].keys():
                    assert key in measurement.keys()

    def test_no_failed_requests(self, fileName):
        for device in self.getContents(fileName):
            for measurement in device['fragmentSeries']:
                assert measurement['count'] >= 0
