
## ChargePlace Scotland API

December 2022 data is missing

## Carbon Intensity API

### Generation Mix

Generation mix for each local authority is measured based on a sample of a postcode. Alternatively, this can be achieved by region query, based on the council_areas.csv table

<details>
  <summary>Click to expand the table</summary>

| Local Authority         | Region ID |
|-------------------------|-----------|
| Aberdeen City           | 1         |
| Aberdeenshire           | 1         |
| Angus                   | 1         |
| Argyll and Bute         | 2         |
| Dumfries and Galloway   | 2         |
| City of Edinburgh       | 2         |
| Glasgow City            | 2         |
| Dundee City             | 1         |
| Falkirk                 | 2         |
| East Ayrshire           | 2         |
| Clackmannanshire        | 2         |
| Highland                | 1         |
| East Dunbartonshire     | 2         |
| Fife                    | 2         |
| East Lothian            | 2         |
| East Renfrewshire       | 2         |
| Inverclyde              | 2         |
| Midlothian              | 2         |
| Moray                   | 1         |
| Perth and Kinross       | 1         |
| Na h-Eileanan an Iar    | 1         |
| North Lanarkshire       | 2         |
| North Ayrshire          | 2         |
| Orkney Islands          | 1         |
| Renfrewshire            | 2         |
| Scottish Borders        | 2         |
| Shetland Islands        | 1         |
| South Ayrshire          | 2         |
| South Lanarkshire       | 2         |
| Stirling                | 1         |
| West Dunbartonshire     | 2         |
| West Lothian            | 2         |

</details>

To run, use CarbonIntensityAPI. Example query would be:

```python
carbon_data, gen_mix_data = api.between(datetime(2022, 10, 1, 0, 0, tzinfo=pytz.utc),
datetime(2022, 10, 31, 23, 30, tzinfo=pytz.utc),
type="postcode", postcode='G5')
```

Printing carbon_data and gen_mix_data, shows our results:

<details>
  <summary>Click to expand the results</summary>

```python
                     timestamp  regionid  forecast  actual     index
0    2022-10-01 00:00:00+00:00         2         1     NaN  very low
1    2022-10-01 00:30:00+00:00         2         0     NaN  very low
2    2022-10-01 01:00:00+00:00         2         0     NaN  very low
3    2022-10-01 01:30:00+00:00         2         0     NaN  very low
4    2022-10-01 02:00:00+00:00         2         0     NaN  very low
...                        ...       ...       ...     ...       ...
1483 2022-10-31 21:30:00+00:00         2        24     NaN  very low
1484 2022-10-31 22:00:00+00:00         2        25     NaN  very low
1485 2022-10-31 22:30:00+00:00         2        22     NaN  very low
1486 2022-10-31 23:00:00+00:00         2        21     NaN  very low
1487 2022-10-31 23:30:00+00:00         2        30     NaN  very low

[1488 rows x 5 columns]
                     timestamp  regionid  biomass  ...  hydro  solar  wind
0    2022-10-01 00:00:00+00:00         2      0.3  ...    2.0    0.0  83.0
1    2022-10-01 00:30:00+00:00         2      0.3  ...    1.5    0.0  83.9
2    2022-10-01 01:00:00+00:00         2      0.3  ...    2.5    0.0  82.6
3    2022-10-01 01:30:00+00:00         2      0.3  ...    2.5    0.0  82.4
4    2022-10-01 02:00:00+00:00         2      0.3  ...    2.3    0.0  82.7
...                        ...       ...      ...  ...    ...    ...   ...
1483 2022-10-31 21:30:00+00:00         2      2.2  ...   11.7    0.0  58.4
1484 2022-10-31 22:00:00+00:00         2      1.6  ...   10.2    0.0  58.5
1485 2022-10-31 22:30:00+00:00         2      1.5  ...    9.2    0.0  60.1
1486 2022-10-31 23:00:00+00:00         2      2.2  ...    7.3    0.0  61.2
1487 2022-10-31 23:30:00+00:00         2      2.7  ...    7.2    0.0  58.3

[1488 rows x 11 columns]

Process finished with exit code 0

```
</details>

### Carbon Intensity Factors

Carbon intensity factors for each fuel type and imports collected from Carbon Intensity API documentation ([donwload link](https://github.com/carbon-intensity/methodology/raw/master/Regional%20Carbon%20Intensity%20Forecast%20Methodology.pdf))

For imports and gas, the values have been averaged. Final intensity factors table used here is:

| Fuel Type | Carbon Intensity   |
|-----------|--------------------|
| Biomass   | 120                |
| Coal      | 937                |
| Imports   | 291                |
| Gas       | 522.5              |
| Nuclear   | 0                  |
| Other     | 300                |
| Hydro     | 0                  |
| Solar     | 0                  |
| Wind      | 0                  |

