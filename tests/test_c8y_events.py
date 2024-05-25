import pytest
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from src.cumulocity import MonthlyEvents
from src.utils import readFile, listFileNames


@pytest.mark.parametrize("folder", ['events/total/', 'events/type/', 'events/typeFragment/'])
def test_all_files_present(folder):
    c8y_data = readFile("c8y_data.json")
    expected_files = set()

    startingDate = max([parse(d['latestEvent']['time']).date() for d in c8y_data if d['latestEvent']])
    lastDate = min([parse(d['oldestEvent']['time']).date() for d in c8y_data if d['oldestEvent']])
    currentDate = startingDate
    while lastDate <= currentDate <= startingDate:
        expected_files.add(f"{folder}{MonthlyEvents.fileName(currentDate.year, currentDate.month)}")
        currentDate -= relativedelta(months=1)

    missingMonthData = expected_files - set(listFileNames(folder))

    assert len(missingMonthData) == 0, f"Missing data for files: {expected_files}"


@pytest.mark.parametrize("fileName", listFileNames('events/total'))
class TestTotalEvents:
    folder = 'total'
    example = {
        "deviceId": 11904,
        "deviceType": "com_cityntel_light",
        "total": {
            "count": 15660,
            "event": "<measurement>"
        }
    }

    def test_required_keys(self, fileName):
        for device in readFile(fileName):
            for key in self.example.keys():
                assert key in device.keys()
            for key in self.example['total'].keys():
                assert key in device['total'].keys()

    def test_no_failed_requests(self, fileName):
        failedEventCount = 0
        for device in readFile(fileName):
            if device['total']['count'] < 0:
                failedEventCount += 1
        assert failedEventCount == 0, f"Devices with failed requests: {failedEventCount}"


@pytest.mark.parametrize("fileName", listFileNames('events/type', folderPrefix=False))
def test_count_matches_total(fileName):
    totalEventsSum = sum([event['total']['count'] for event in readFile(f"events/total/{fileName}")])
    eventTypesSum = 0
    for obj in readFile(f"events/type/{fileName}"):
        for event in obj['eventByType']:
            eventTypesSum += event['count']

    assert totalEventsSum == eventTypesSum, f"Total event count doesn't match for {fileName}"


@pytest.mark.parametrize("fileName", listFileNames('events/type', folderPrefix=False))
def test_fragment_count_exceeds_total(fileName):
    totalEventsSum = sum([event['total']['count'] for event in readFile(f"events/total/{fileName}")])

    typeFragmentSum = 0
    for obj in readFile(f"events/typeFragment/{fileName}"):
        for event in obj['typeFragment']:
            typeFragmentSum += event['count']

    assert totalEventsSum <= typeFragmentSum, f"Missing typeFragment data for {fileName}"


@pytest.mark.parametrize("fileName", listFileNames('events/type'))
class TestEventType:
    folder = 'type'
    example = {
        "deviceId": 11904,
        "deviceType": "com_cityntel_light",
        "eventByType": [
            {
                "type": "com_cityntel_status_data",
                "count": 15660,
                "event": "<measurement>"
            }
        ]
    }

    def test_required_keys(self, fileName):
        for device in readFile(fileName):
            for key in self.example.keys():
                assert key in device.keys()
            for event in device['eventByType']:
                for key in self.example['eventByType'][0].keys():
                    assert key in event.keys()

    def test_no_failed_requests(self, fileName):
        failedEventCount = 0

        for device in readFile(fileName):
            hasFailedEvents = False
            for event in device['eventByType']:
                if event['count'] < 0:
                    hasFailedEvents = True
            if hasFailedEvents:
                failedEventCount += 1
        assert failedEventCount == 0, f"Devices with failed requests: {failedEventCount}"


@pytest.mark.parametrize("fileName", listFileNames('events/typeFragment'))
class TestEventType:
    example = {
        "deviceId": 11904,
        "deviceType": "com_cityntel_light",
        "typeFragment": [
            {
                "type": "com_cityntel_status_data",
                'fragment': 'com_cityntel_status_data',
                "count": 15660,
                "event": "<measurement>"
            }
        ]
    }
    folder = 'typeFragment'

    def test_required_keys(self, fileName):
        for device in readFile(fileName):
            for key in self.example.keys():
                assert key in device.keys()
            for event in device['typeFragment']:
                for key in self.example['typeFragment'][0].keys():
                    assert key in event.keys()

    def test_no_failed_requests(self, fileName):
        failedEventCount = 0

        for device in readFile(fileName):
            hasFailedEvents = False
            for event in device['typeFragment']:
                if event['count'] < 0:
                    hasFailedEvents = True
            if hasFailedEvents:
                failedEventCount += 1
        assert failedEventCount == 0, f"Devices with failed requests: {failedEventCount}"
