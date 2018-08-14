# Freeze Layer

Utility to create a copy of a layer and the data necessary to render it, with a
new start and end time query.

## Install

```
git clone https://github.com/fgassert/rw-freeze-layer.git
cd rw-freeze-layer
pip install -r requirements.txt
```

## Usage

**Option 1. With Docker**

1. Save your keys to an environment file `.env`
```
CARTO_USER=<username>
CARTO_KEY=<carto_key>
RW_API_KEY=<rw_api_key>
```

2. Start script builds and runs a container with interactive.
`./start.sh`


**Option 2. Command line interative** 

Run `python freeze`.

Example output 
```
Use test enviornment (https://staging-api.globalforestwatch.org/v1/)? (Y/n) n
Using production: https://api.resourcewatch.org/v1/

ID of Layer to freeze:
> a5136895-9aab-4f2c-8a33-d22b833724ec
Found layer: Average PM 2.5 Concentration (µg/m³) (Past 24 Hours)

Enter start date for freeze
(YYYY-MM-DD | today | yesterday):
> yesterday
Query start: Sat Aug 11 00:00:00 2018

Enter end date for freeze
(YYYY-MM-DD | today | yesterday):
> today
End date is more recent than the latest data in the table! The frozen dataset will not update to include that data if it is added in the future.
Continue anyway? (y/N) y
Query end: Sun Aug 12 00:00:00 2018

Created new layer.
Layer Id: cb7fcfb6-b27f-4040-bf41-17eadd8de9cb
Layer name: Average PM 2.5 Concentration (µg/m³) (Past 24 Hours) (2018-08-11T00:00:00 to 2018-08-12T00:00:00)
http://resourcewatch.org/admin/data/layers/cb7fcfb6-b27f-4040-bf41-17eadd8de9cb

Created new table.
Table name: "rw-nrt".cit_003a_air_quality_pm25_20180811_0000_20180812_0000

Keep new layer and table? (Y/n) n
Deleted layer: cb7fcfb6-b27f-4040-bf41-17eadd8de9cb
Dropped table: cit_003a_air_quality_pm25_20180811_0000_20180812_0000
```

**Option 3. Python**

``` python
import freezeLayer
import datetime

# Layer to copy
layerId = 'a5136895-9aab-4f2c-8a33-d22b833724ec'

# Past 7 days starting from yesterday UTC midnight
start = datetime.datetime.utcnow().replace(hour=0, minute=0) - datetime.timedelta(days=8)
end = datetime.datetime.utcnow().replace(hour=0, minute=0) - datetime.timedelta(days=1)

lyr, table = freezeLayer.freezeLayer(layerId, start, end)

print(lyr)
print(table)
```
