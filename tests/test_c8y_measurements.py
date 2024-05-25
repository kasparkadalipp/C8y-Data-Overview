import pytest
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from src.cumulocity import MonthlyMeasurements
from src.utils import readFile, listFileNames


@pytest.mark.parametrize("folder",
                         ['measurements/total', 'measurements/fragmentSeries', 'measurements/typeFragmentSeries'])
def test_all_files_present(folder):
    c8y_data = readFile("c8y_data.json")
    expected_files = set()

    startingDate = max([parse(d['latestMeasurement']['time']).date() for d in c8y_data if d['latestMeasurement']])
    lastDate = min([parse(d['oldestMeasurement']['time']).date() for d in c8y_data if d['oldestMeasurement']])
    currentDate = startingDate
    while lastDate <= currentDate <= startingDate:
        expected_files.add(f"{folder}{MonthlyMeasurements.fileName(currentDate.year, currentDate.month)}")
        currentDate -= relativedelta(months=1)
    missingMonthData = expected_files - set(listFileNames(folder))
    assert len(missingMonthData) == 0, f"Missing data for files: {expected_files}"


@pytest.mark.parametrize("fileName", listFileNames('measurements/total', folderPrefix=False))
class TestValidateEventCount:
    def test_type_count_matches_total(self, fileName):
        fragmentSeriesSum = 0
        for obj in readFile(f"measurements/fragmentSeries/{fileName}"):
            for measurement in obj['fragmentSeries']:
                fragmentSeriesSum += measurement['count']
        typeFragmentSeriesSum = 0
        for obj in readFile(f"measurements/typeFragmentSeries/{fileName}"):
            for measurement in obj['typeFragmentSeries']:
                typeFragmentSeriesSum += measurement['count']
        assert fragmentSeriesSum == typeFragmentSeriesSum, f"Missing types for measurements/typeFragmentSeries/{fileName}"

    def test_fragment_count_exceeds_total(self, fileName):
        totalMeasurementSum = sum([event['total']['count'] for event in readFile(f"measurements/total/{fileName}")])
        fragmentSeriesSum = 0
        for obj in readFile(f"measurements/fragmentSeries/{fileName}"):
            for measurement in obj['fragmentSeries']:
                fragmentSeriesSum += measurement['count']
        assert totalMeasurementSum <= fragmentSeriesSum, f"Missing data for measurements/fragmentSeries/{fileName}"


@pytest.mark.parametrize("fileName", listFileNames('measurements/total'))
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

    def test_required_keys(self, fileName):
        for device in readFile(fileName):
            for key in self.example.keys():
                assert key in device.keys()
            for key in self.example['total'].keys():
                assert key in device['total'].keys()

    def test_no_failed_requests(self, fileName):
        failedRequests = 0
        deviceIds = set()
        for device in readFile(fileName):
            if device['total']['count'] < 0:
                failedRequests += 1
                deviceIds.add(device['deviceId'])
        assert failedRequests == 0, f"For devices: {deviceIds}"


@pytest.mark.parametrize("fileName", listFileNames('measurements/fragmentSeries'))
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

    def test_required_keys(self, fileName):
        for device in readFile(fileName):
            for key in self.example.keys():
                assert key in device.keys()

            for measurement in device['fragmentSeries']:
                for key in self.example['fragmentSeries'][0].keys():
                    assert key in measurement.keys()

    def test_no_failed_requests(self, fileName):
        failedRequests = 0
        deviceIds = set()
        for device in readFile(fileName):
            for measurement in device['fragmentSeries']:
                if measurement['count'] < 0:
                    failedRequests += 1
                    deviceIds.add(device['deviceId'])
        assert failedRequests == 0, f"For devices: {deviceIds}"


@pytest.mark.parametrize("fileName", listFileNames('measurements/typeFragmentSeries'))
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

    def test_required_keys(self, fileName):
        for device in readFile(fileName):
            for key in self.example.keys():
                assert key in device.keys()

            for measurement in device['typeFragmentSeries']:
                for key in self.example['typeFragmentSeries'][0].keys():
                    assert key in measurement.keys()

    def test_no_failed_requests(self, fileName):
        deviceIds = set()
        failedRequests = 0
        for device in readFile(fileName):
            for measurement in device['typeFragmentSeries']:
                if measurement['count'] < 0:
                    failedRequests += 1
                    deviceIds.add(device['deviceId'])
        assert failedRequests == 0, f"For devices: {deviceIds}"
