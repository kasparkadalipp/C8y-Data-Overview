from .config import getCumulocityApi
from .inventory import Inventory
from .events import Events, MonthlyEvents
from .measurements import Measurements, MonthlyMeasurements, requestMonthBounds

from .requests.groups import getDeviceGroups
from .requests.devices import Devices

from .requests.eventsTotal import requestMonthlyData as eventsTotal
from .requests.eventsType import requestMonthlyData as eventsType
from .requests.eventsTypeFragment import requestMonthlyData as eventsTypeFragment

from .requests.measurementTotal import requestMonthlyData as measurementsTotal
from .requests.measurementFragmentSeries import requestMonthlyData as measurementsFragmentSeries
from .requests.measurementTypeFragmentSeries import requestMonthlyData as measurementTypeFragmentSeries
