from tqdm import tqdm

from .config import getCumulocityApi
from src.utils import tqdmFormat


def getDeviceGroups():
    c8y = getCumulocityApi()

    removedKeys = ['childDevices', 'childAssets', 'additionParents', 'childAdditions', 'deviceParents', 'assetParents',
                   'self']

    parameters = {
        'pageSize': 2000,
        'withChildren': True,
        'withParents': True,
        'query': "$filter="
                 "has(c8y_IsDeviceGroup) "
                 "or type eq 'c8y_DeviceGroup' "
        # "or has(c8y_IsDynamicGroup)" 
        # "or type eq 'c8y_DynamicGroup' "
        # "or has(c8y_DeviceSubgroup)"
        # "or type eq 'c8y_DeviceSubgroup' "
                 "$orderby=c8y_IsDeviceGroup desc"

        # default query
        # 'query': "$filter=((has(c8y_IsDeviceGroup)) or ((type eq 'c8y_DynamicGroup')
        # and (has(c8y_IsDynamicGroup)) and (not(has(c8y_IsDynamicGroup.invisible))))) $orderby=c8y_IsDeviceGroup desc"
    }

    with tqdm(desc=f'Requesting groups', bar_format=tqdmFormat) as pbar:
        response = c8y.get(resource="/inventory/managedObjects", params=parameters)
        total = len(response['managedObjects'])
        pbar.total = total
        pbar.n = total
        pbar.refresh()

    groups = set([group['id'] for group in response['managedObjects']])

    data = []
    for obj in response['managedObjects']:
        parentAssets = obj['assetParents']['references']
        childAssets = obj['childAssets']['references']

        group = {key: item for key, item in obj.items() if key not in removedKeys}
        group = {**group, 'depth': len(parentAssets), 'parentId': None, 'subgroups': [], 'parents': [],
                 'childDevices': []}

        for parent in parentAssets:
            parent = parent['managedObject']
            group['parents'].append({"id": parent['id'], 'name': parent['name']})

        for parent in childAssets:
            child = parent['managedObject']
            if child['id'] in groups:
                group['subgroups'].append({"id": child['id'], 'name': child['name']})
            else:
                group['childDevices'].append({"id": child['id'], 'name': child['name']})
        data.append(group)

    groupMapping = set([item['id'] for item in data if item['depth'] == 0])
    for group in sorted(data, key=lambda x: x['depth']):
        if group['depth'] == 0:
            group['parentId'] = "root"

        for parent in group['parents']:
            if parent['id'] in groupMapping:
                group['parentId'] = parent['id']
                groupMapping.add(group['id'])
                break
    return data
