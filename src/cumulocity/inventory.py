from tqdm import tqdm

from .config import getCumulocityApi
from src.utils import tqdmFormat

c8y = getCumulocityApi()


def requestDeviceInventory():
    c8y_devices = _requestDeviceInventory()
    return _convertInventoryToJson(c8y_devices)


def _convertInventoryToJson(c8y_devices):
    '''
    Converts Cumulocity-python-api.ManagedObject -> json
    '''
    data = []
    for managedObject in c8y_devices:
        device = managedObject['device']

        data.append({
            'id': device.id,
            'type': device.type,
            'name': device.name,
            'owner': device.owner,
            'creationTime': device.creation_time,
            'lastUpdated': device.update_time,

            'is_device': 'c8y_IsDevice' in device,
            'is_group': 'c8y_IsDeviceGroup' in device,
            'child_devices': [child.to_json() for child in device.child_devices],
            'child_additions': [child.to_json() for child in device.child_additions],
            'child_assets': [child.to_json() for child in device.child_assets],
            'c8y_inventory': device.to_json(),
            'depth': managedObject['depth'],
            'parent': managedObject['parent'] if 'parent' in managedObject else ''
        })
    return data


def requestSupportedMeasurements(c8y, deviceId: str | int):
    result = set()
    supportedFragments = c8y.inventory.get_supported_measurements(deviceId)  # fragment
    supportedSeries = c8y.inventory.get_supported_series(deviceId)  # fragment.series or just series

    for fragment in supportedFragments:
        for fullName in supportedSeries:
            if fragment == fullName:
                result.add((fragment, fullName))

            elif fullName.startswith(fragment):
                series = fullName[len(fragment):]
                if series.startswith('.'):
                    series = series[1:]
                    result.add((fragment, series))
    return [{'fragment': fragment, 'series': series} for fragment, series in result]


def _requestDeviceInventory():
    c8y = getCumulocityApi()

    depth = 0
    c8y_devices = []
    for device in tqdm(c8y.device_inventory.get_all(), desc=f'Requesting device inventory for depth {depth}',
                       bar_format=tqdmFormat):
        c8y_devices.append({'depth': depth, 'device': device})

    current_devices = c8y_devices
    uniqueIds = set([obj['device'].id for obj in c8y_devices])
    while True:
        depth += 1
        device_children = _listChildDevices(current_devices)
        if not device_children:
            break

        uniqueDevices = []
        for child in device_children:
            if child['id'] in uniqueIds:
                continue
            uniqueIds.add(child['id'])
            uniqueDevices.append(child)
        device_children = uniqueDevices

        for child in tqdm(device_children, desc=f'Requesting device inventory for depth {depth}',
                          bar_format=tqdmFormat):
            child['depth'] = depth
            child['device'] = c8y.device_inventory.get(child['id'])

        c8y_devices += device_children
        current_devices = device_children
    return c8y_devices


def _listChildDevices(devices):
    device_children = []
    for parentObj in devices:
        parent = parentObj['device']

        child_devices = []
        for child in parent.child_devices:
            child_devices.append({
                'id': child.id,
                'parent': parent.id
            })
        device_children += child_devices
    return device_children
