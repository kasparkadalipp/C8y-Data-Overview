from collections import defaultdict
from src.utils import readFile, listFileNames, getPath
from wordcloud import WordCloud


def wordCloudDeviceNames():
    c8y_data = readFile('c8y_data.json')
    wordcloudInput = ','.join([device['name'] for device in c8y_data]).replace("_", ' ')

    wordcloud = WordCloud(background_color="white", max_words=5000, contour_width=3, contour_color='steelblue',
                          width=1600, height=800)
    wordcloud.generate(wordcloudInput)
    wordcloud.to_file(getPath('wordcloud device names.png'))

    return wordcloud.to_image()


def wordCloudDevices():
    c8y_data = readFile('c8y_data.json')
    deviceIdMapping = {device['id']: device for device in c8y_data}

    uniqueValues = defaultdict(set)
    for fileName in listFileNames('measurements/typeFragmentSeries/'):
        for device in readFile(fileName):
            deviceId = device['deviceId']
            deviceName = deviceIdMapping[deviceId]['name']

            for fragmentSeries in device['typeFragmentSeries']:
                measurementType = fragmentSeries['type']
                fragment = fragmentSeries['fragment']
                series = fragmentSeries['series']
                count = fragmentSeries['count']

                measurement = fragmentSeries['measurement']
                if measurement:
                    measurementValue = measurement[fragment][series]
                    unit = measurementValue['unit'] if 'unit' in measurementValue else ''
                    unit = '' if unit is None else unit

                    key = (deviceId, deviceName, measurementType, fragment, series)
                    key = tuple('' if value is None or not str(value) else value for value in key)

                    uniqueValues[key].add(unit)

    result = {}
    for key, units in uniqueValues.items():
        deviceId, deviceName, measurementType, fragment, series = key

        if deviceId not in result:
            result[deviceId] = {
                "device": deviceName,
                'fragments': [],
                'series': [],
                'units': []
            }
        else:
            device = result[deviceId]
            device['fragments'].append(fragment)
            device['series'].append(series)
            device['units'] += list(set(units))

    wordcloudInput = []
    for value in result.values():
        deviceName = value['device']
        fragments = ','.join(value['fragments'])
        series = ','.join(value['series'])
        units = ','.join(value['units'])
        wordcloudInput.append(f'{deviceName} {fragments} {series} {units}')
    wordcloudInput = ','.join(wordcloudInput).replace("_", ' ')

    wordcloud = WordCloud(background_color="white", max_words=5000, contour_width=3, contour_color='steelblue',
                          width=1600,
                          height=800)
    wordcloud.generate(wordcloudInput)
    wordcloud.to_file(getPath('wordcloud measurements.png'))
