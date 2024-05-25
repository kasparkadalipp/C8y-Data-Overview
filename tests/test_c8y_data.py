import pytest
from dateutil.parser import parse, ParserError
from src.utils import readFile

c8y_data = readFile('c8y_data.json')

example_c8y_data = {
    "id": "11904",
    "type": "com_cityntel_light",
    "name": "light 95 (039A)",
    "owner": "device_com_cityntel_live",
    "creationTime": "2018-09-20T03:28:05.273+03:00",
    "lastUpdated": "2024-04-18T05:25:12.130+03:00",
    "is_device": True,
    "is_group": False,
    "depth": 1,
    "parent": "10503",
    "c8y_inventory": ["<additional custom objects>"],

    "c8y_supportedSeries": [
        {
            "fragment": "Light",
            "series": "output"
        }
    ],
    "dataAfter": "1970-01-01",
    "dataBefore": "2024-04-01",

    "measurementCount": 39878,
    "latestMeasurement": "<measurement>",
    "oldestMeasurement": "<measurement>",

    "eventCount": 40192,
    "latestEvent": "<event>",
    "oldestEvent": "<event>",
}


@pytest.mark.parametrize("required_key", example_c8y_data.keys())
def test_key_present(required_key):
    assert all([required_key in device for device in c8y_data])


def test_unique_id():
    ids = set([device['id'] for device in c8y_data])
    assert len(ids) == len(c8y_data)


def test_valid_parent_id():
    rootDeviceParent = ''
    ids = set([device['id'] for device in c8y_data] + [rootDeviceParent])

    for device in c8y_data:
        assert device['parent'] in ids, f'Invalid device inventory structure, {device['id']}'


def test_supported_series_has_measurements():
    for device in c8y_data:
        if device['c8y_supportedSeries']:
            assert device['measurementCount'] > 0, f"{device['id']} - device should have at least 1 measurement"


def test_missing_measurement_values():
    for device in c8y_data:
        assert not device['measurementCount'] == -1, f"{device['id']}"


def test_missing_event_values():
    for device in c8y_data:
        assert not device['eventCount'] == -1, f"{device['id']}"
        assert not device['eventCountValidation'] == -1, f"{device['id']}"


def test_valid_time_format():
    for device in c8y_data:
        for dataObject in ['latestMeasurement', 'oldestMeasurement', 'oldestEvent', 'latestEvent']:
            if not device[dataObject]:
                continue
            dateString = device[dataObject]['time']
            try:
                parse(dateString)
            except ParserError:
                pytest.fail(f"Parsing failed for date string: {dateString}")


def test_validate_oldest_and_latest_measurement_order():
    for device in c8y_data:
        oldest = device['oldestMeasurement']
        latest = device['latestMeasurement']
        if oldest and latest:
            dateFrom = parse(oldest['time']).date()
            dateTo = parse(latest['time']).date()
            assert dateFrom <= dateTo, f"Device {device['id']} measurement order mismatch: {dateFrom} > {dateTo}"


def test_validate_oldest_and_latest_event_order():
    for device in c8y_data:
        earliest = device['oldestEvent']
        latest = device['latestEvent']
        if earliest and latest:
            dateFrom = parse(earliest['time']).date()
            dateTo = parse(latest['time']).date()
            assert dateFrom <= dateTo, f"Device {device['id']} event order mismatch: {dateFrom} > {dateTo}"


def test_data_has_ignored_events():
    for device in c8y_data:
        assert 'ignoredEvent' not in device, f"Fix requesting data for {device['id']}"
