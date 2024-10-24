<br />
<div align="center">
  <a href="https://github.com/othneildrew/Best-README-Template">
    <img src="images/ut_logo.png" alt="Logo" width="120" height="120">
  </a>

  <h3 align="center">Knowledge Graphs for Cataloging and Making Sense of Smart City Data</h3>

  <p>
    <a href="https://kasparkadalipp.github.io/C8y-Data-Overview/visualization/measurementsMonthly/">Monthly Measurements Visualization</a>
    ·
    <a href="https://kasparkadalipp.github.io/C8y-Data-Overview/visualization/deltaMeasurements/">Delta Measurements Visualization</a>
  </p>
  <p>
    <a href="https://kasparkadalipp.github.io/C8y-Data-Overview/visualization/measurementsTotal/">Total Measurements Visualization</a>
    ·
    <a href="https://kasparkadalipp.github.io/C8y-Data-Overview/visualization/measurementsTotalFiltered/">(Filtered)</a>
    ·
    <a href="https://kasparkadalipp.github.io/C8y-Data-Overview/visualization/eventsTotal/">Total Events Visualization </a>
  </p>
</div>


### Built with

* [![Python][Python.org]][Python-url]
* [![D3][D3.js]][D3-url]
  



<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[Python.org]: https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54
[Python-url]: https://www.python.org/
[D3.js]: https://img.shields.io/badge/D3.js-F9A03C?logo=d3dotjs&logoColor=fff&style=for-the-badge
[D3-url]: https://d3js.org/
[ChatGPT]: https://img.shields.io/badge/chatGPT-74aa9c?logo=openai&logoColor=white&style=for-the-badge
[ChatGPT-url]: https://platform.openai.com/docs/api-reference

## Examples

You can find examples in the [notebooks](https://github.com/kasparkadalipp/C8y-Data-Overview/tree/main/notebooks) folder.

## Prerequisites

Before you begin, ensure you have met the following requirements:

- A Cumulocity account with sufficient permissions to request data schemas.
- An OpenAI API key with sufficient funds for dataset transformation.
- Python 3.12
- Necessary Python libraries installed (see the `requirements.txt` file).

## Environment Variables

Set the following environment variables in the `.env` file in the project root.

```bash
# Cumulocity credentials
C8Y_USERNAME=firstname.lastname
C8Y_PASSWORD=password
C8Y_TENANT_URL=sample.platvorm.iot.ee
C8Y_TENANT_ID=t12345

# OpenAI credentials
OPENAI_API_KEY=dGhpcyBpcyBhIHNhbXBsZSBBUEkga2V5
OPENAI_ORGANIZATION_ID=org-b3JnYW5pemF0aW9uIGlk

# Folder used to save query results
DATA_FOLDER=example
```

## Saved data

Results get saved in the **/data** folder as JSON.

For a detailed description of the data format, please refer to the [Saved Data Format Documentation](/docs/README.md).


