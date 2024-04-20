# Saved data format

### c8y_data

```JSON
{
  "id": "11904",
  "type": "com_cityntel_light",
  "name": "light 95 (039A)",
  "owner": "device_com_cityntel_live",
  "creationTime": "2018-09-20T03:28:05.273+03:00",
  "lastUpdated": "2024-04-18T05:25:12.130+03:00",
  "is_device": true,
  "is_group": false,
  "depth": 1,
  "parent": "10503",
  "c8y_inventory": [ <additional custom objects> ]

  "c8y_supportedSeries": [{
      "fragment": "Light",
      "series": "output"
    }
  ],
  "dataAfter": "1970-01-01",
  "dataBefore": "2024-04-01",

  "lastMeasurement": <measurement>,
  "measurementCount": 39878,

  "firstMeasurement": <measurement>,
  "measurementCountValidation": 39878,

  "lastMeasurementValidation": <measurement>,

  "lastEvent": <event>,
  "eventCount": 40192,

  "firstEvent": <event>,
  "eventCountValidation": 40192
}
```

### c8y_measurements (total)

```JSON
{
  "deviceId": 11904,
  "deviceType": "com_cityntel_light",
  "total": {
    "count": 15660,
    "lastMeasurement": <measurement>
  }
}

```

### c8y_measurements (fragment, series)

```JSON
{
  "deviceId": 11904,
  "deviceType": "com_cityntel_light",
  "fragmentSeries": [
    {
      "fragment": "active_power",
      "series": "L1",
      "count": 2304,
      "lastMeasurement": <measurement>
    }
  ]
}

```

### c8y_measurements (type, fragment, series)

```JSON
{
  "id": 11904,
  "deviceType": "com_cityntel_light",
  "typeFragmentSeries": [
    {
      "type": "c4t_metric",
      "fragment": "active_power",
      "series": "L1",
      "count": 150,
      "lastMeasurement": <measurement>
    }
  ]
}

```

### c8y_groups

```JSON
{
  "id": "692449576",
  "name": "Traffic Counters",
  "type": "c8y_DeviceSubgroup",
  "owner": "jaks",
  "creationTime": "2021-06-08T14:37:19.235+03:00",
  "lastUpdated": "2021-06-08T14:37:19.235+03:00",
  "c8y_IsDeviceGroup": {},
  "depth": 1,
  "parentId": "692449575",
  "subgroups": [
    {
      "id": "692449577",
      "name": "AVC"
    },
    {
      "id": "692453862",
      "name": "Thinnect"
    },
    {
      "id": "692453863",
      "name": "Bercman"
    },
    {
      "id": "692455302",
      "name": "ECO"
    },
    {
      "id": "695346567",
      "name": "Thinnect 2021"
    },
    {
      "id": "695350904",
      "name": "Thinnect 2020"
    }
  ],
  "parents": [
    {
      "id": "692449575",
      "name": "Modal Split"
    }
  ],
  "childDevices": []
}
```
