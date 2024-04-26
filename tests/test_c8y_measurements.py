import pytest
import json
import os

from datetime import date
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta

load_dotenv('../notebooks/.env.telia')
from src.cumulocity import MonthlyMeasurements
from src.utils import pathExists

basePath = "../data/telia/measurements/"


def getFiles(folder):
    return os.listdir(basePath + folder)


@pytest.mark.parametrize("folder", ['total', 'fragmentSeries', 'typeFragmentSeries'])
def test_all_files_present(folder):
    assert pathExists(basePath + folder), f'Path "{basePath + folder}" does not exist'
    earliestMeasurementDate = date(2014, 11, 1)
    oldestMeasurementDate = date(2024, 3, 1)
    expected_files = set()

    currentDate = earliestMeasurementDate
    while currentDate <= oldestMeasurementDate:
        expected_files.add(MonthlyMeasurements.fileName(currentDate.year, currentDate.month))
        currentDate += relativedelta(months=1)

    for file in getFiles(folder):
        if file in expected_files:
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
        failedRequests = 0
        deviceIds = set()
        for device in self.getContents(fileName):
            if device['total']['count'] < 0:
                failedRequests += 1
                deviceIds.add(device['deviceId'])
        assert failedRequests == 0, f"For devices: {deviceIds}"

    def test_month_with_no_active_devices(self, fileName):
        activeDevices = 0
        for device in self.getContents(fileName):
            count = device['total']['count']
            if count > 0:
                activeDevices += 1
        assert activeDevices > 0


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
        failedRequests = 0
        deviceIds = set()
        for device in self.getContents(fileName):
            for measurement in device['fragmentSeries']:
                if measurement['count'] < 0:
                    failedRequests += 1
                    deviceIds.add(device['deviceId'])
        assert failedRequests == 0, f"For devices: {deviceIds}"

    def test_month_with_no_active_devices(self, fileName):
        activeDevices = 0
        for device in self.getContents(fileName):
            for measurement in device['fragmentSeries']:
                if measurement['count'] != 0:
                    activeDevices += 1
        assert activeDevices > 0


@pytest.mark.parametrize("fileName", getFiles('typeFragmentSeries'))
class TestTypeFragmentSeries:
    folder = 'typeFragmentSeries'
    example = {
        "deviceId": 11904,
        "deviceType": "com_cityntel_light",
        "typeFragmentSeries": [
            {
                "type": "com_cityntel_light",
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

            for measurement in device['typeFragmentSeries']:
                for key in self.example['typeFragmentSeries'][0].keys():
                    assert key in measurement.keys()

    def test_no_failed_requests(self, fileName):
        deviceIds = set()
        failedRequests = 0
        for device in self.getContents(fileName):
            for measurement in device['typeFragmentSeries']:
                if measurement['count'] < 0:
                    failedRequests += 1
                    deviceIds.add(device['deviceId'])
        assert failedRequests == 0, f"For devices: {deviceIds}"

    def test_month_with_no_active_devices(self, fileName):
        activeDevices = 0
        for device in self.getContents(fileName):
            for measurement in device['typeFragmentSeries']:
                if measurement['count'] != 0:
                    activeDevices += 1
        assert activeDevices > 0
