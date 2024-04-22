import pytest
import json
import os

from datetime import date
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta

basePath = "../data/telia/events/"

load_dotenv('../notebooks/.env.telia')
from src.cumulocity import MonthlyEvents
from src.utils import pathExists


def getFiles(folder):
    return os.listdir(basePath + folder)


@pytest.mark.parametrize("folder", ['total', 'fragmentSeries', 'typeFragmentSeries'])
def test_all_files_present(folder):
    assert pathExists(basePath + folder), f'Path "{basePath + folder}" does not exist'
    earliestEventDate = date(2016, 11, 1)
    oldestEventDate = date(2024, 3, 1)
    expected_files = set()

    currentDate = earliestEventDate
    while currentDate <= oldestEventDate:
        expected_files.add(MonthlyEvents.fileName(currentDate.year, currentDate.month))
        currentDate += relativedelta(months=1)

    for file in getFiles(folder):
        expected_files.remove(file)
    missingFilesCount = len(expected_files)

    assert missingFilesCount == 0, f"Missing data for files: {expected_files}"


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
