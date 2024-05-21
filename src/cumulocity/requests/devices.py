from datetime import date
from tqdm import tqdm
from src.cumulocity import Inventory, Measurements, Events
from src.utils import tqdmFormat


class Devices:
    def __init__(self, dateFrom: date, dateTo: date, c8y_data: list = None):
        self.dateFrom = dateFrom
        self.dateTo = dateTo
        self.Inventory = Inventory()

        if c8y_data:
            self.c8y_data = c8y_data
        else:
            self.c8y_data = self.Inventory.requestDeviceInventory()
            print()

        for device in self.c8y_data:
            device['dataAfter'] = self.dateFrom.isoformat()
            device['dataBefore'] = self.dateTo.isoformat()

        if not all(['c8y_supportedSeries' in device.keys() for device in self.c8y_data]):
            self.requestSupportedSeries()

    def initializeData(self):
        self.requestTotalMeasurements()
        self.requestTotalEvents()
        return self

    def requestSupportedSeries(self):
        for device in tqdm(self.c8y_data, desc="Requesting supported series", bar_format=tqdmFormat):
            device['c8y_supportedSeries'] = self.Inventory.requestSupportedMeasurements(device['id'])
        return self

    def requestTotalMeasurements(self):
        for device in tqdm(self.c8y_data, desc="Requesting total measurements", bar_format=tqdmFormat):
            cumulocity = Measurements(device)
            response = cumulocity.requestLatestMeasurement(self.dateFrom, self.dateTo)
            device['measurementCount'] = response['count']
            device['latestMeasurement'] = response['latestMeasurement']

            response = cumulocity.requestOldestMeasurement(self.dateFrom, self.dateTo)
            device['oldestMeasurement'] = response['oldestMeasurement']
            # device['measurementCountValidation'] = response['count']
            #
            # device['latestMeasurementValidation'] = cumulocity.requestLatestMeasurementValidation(self.dateTo)
        return self

    def requestTotalEvents(self):
        for device in tqdm(self.c8y_data, desc="Requesting total events", bar_format=tqdmFormat):
            cumulocity = Events(device)
            response = cumulocity.requestLatestEvent(self.dateFrom, self.dateTo)
            device['eventCount'] = response['count']
            device['latestEvent'] = response['latestEvent']

            response = cumulocity.requestOldestEvent(self.dateFrom, self.dateTo)
            device['oldestEvent'] = response['oldestEvent']
            # device['eventCountValidation'] = response['count']
        return self

    @staticmethod
    def fileName():
        return "c8y_data.json"
