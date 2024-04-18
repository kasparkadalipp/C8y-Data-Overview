from tqdm import tqdm

from .config import getCumulocityApi
from src.utils import tqdmFormat


def getDeviceInventory():
    c8y_devices = _requestDeviceInventory()
    return _convertInventoryToJson(c8y_devices)


def _convertInventoryToJson(c8y_devices):
    '''
    Converts Cumulocity-python-api.ManagedObject -> json
    '''
    data = []

    for deviceObj in c8y_devices:
        device = deviceObj['device']

        data.append({
            **deviceObj,
            'device': {
                **device.to_json(),
                "id": device.id,
                "creationTime": device.creation_time,
                "lastUpdated": device.update_time,
                "is_device": 'c8y_IsDevice' in device,
                "is_group": 'c8y_IsDeviceGroup' in device,
                "child_devices": [child.to_json() for child in device.child_devices],
                "child_additions": [child.to_json() for child in device.child_additions],
                "child_assets": [child.to_json() for child in device.child_assets]
            }
        })
    return data


def _requestDeviceInventory():
    c8y = getCumulocityApi()

    depth = 0
    c8y_devices = []
    for device in tqdm(c8y.device_inventory.get_all(), desc=f'Requesting device inventory for depth {depth}',
                       bar_format=tqdmFormat):
        c8y_devices.append({'depth': depth, "device": device})

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
            del child['id']

        c8y_devices += device_children
        current_devices = device_children
    return c8y_devices


def _listChildDevices(devices):
    device_children = []
    for parentObj in devices:
        parent = parentObj["device"]

        child_devices = []
        for child in parent.child_devices:
            child_devices.append({
                "id": child.id,
                "parent": parent.id
            })
        device_children += child_devices
    return device_children
