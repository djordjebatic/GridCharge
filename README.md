# ⚡🔌 GridCharge Dataset Generation Pipeline

This repo contains codebase used to create the GridCharge dataset - capturing carbon intensity data for 5M+ public Electric Vehicle (EV) charging sessions across Scotland, enriched with granular carbon intensity information from the UK National Grid.

## About the Dataset

The GridCharge dataset provides a detailed view of EV charging sessions combined with real-time carbon intensity data at 30-minute granularity. It supports research in smart charging, grid load analysis, carbon-aware energy consumption, and weather impact on charging behavior.

**Key Features:**
- 5M+ public EV charging sessions across Scotland
- 30-minute granularity carbon intensity data
- Geographical organization by Scottish council areas
- Integration with UK National Grid Carbon Intensity API
- Comprehensive tariff information with structured parsing

**Data Availability**

The final dataset is publicly available on Hugging Face:
[huggingface.co/datasets/djordjebatic/GridCharge](https://huggingface.co/datasets/djordjebatic/GridCharge)


## Repository Structure

```
├── data/
│   ├── source/                          # Raw data sources
│   │   ├── geo_data/                    # Geographic data files
│   │   └── scraper.py                   # Data scraping utilities
│   └── result/                          # Generated dataset output
├── src/
│   ├── carbon/                          # Carbon intensity API integration
│   │   ├── carbon_adapter.py            # Caching adapter for API calls
│   │   ├── carbon_client.py             # HTTP client with retry logic
│   │   ├── carbon_intensity_api.py      # Main API interface
│   │   ├── carbon_parser.py             # JSON response parsing
│   │   └── carbon_service.py            # High-level service orchestration
│   └── chargeplace/                     # ChargePlace Scotland integration
│       ├── chargeplace_scotland_api.py  # Main processing pipeline
│       └── sessions.py                  # Session data processing utilities
├── main.py                              # Main execution script
├── requirements.txt                     # Python dependencies
└── setup.py                             # Package setup
```

## Installation

### Setup

**Prerequisites:** Python 3.8


1. **Clone the repository:**
```bash
git clone https://github.com/djordjebatic/GridCharge.git
cd GridCharge
```

2. **Create and activate a virtual environment:**
```bash
conda create --name gridcharge python==3.8
conda activate gridcharge
```

3. **Install dependencies:**
```bash
pip install -r requirements.txt
```

4. **Install the package:**
```bash
pip install -e .
```


## Data Acquisition and Preprocessing

The `scraper.py` script is the entry point for acquiring raw data from ChargePlace Scotland. The `main.py` script orchestrates the entire data processing pipeline, from scraping to final dataset generation.

### 1. Scraping Raw Data (`scraper.py`)

This script is responsible for fetching the necessary raw data from external sources:

*   **ChargePoint API Scraping:**
    *   It interact with the ChargePlace Scotland API endpoints:
        *   `https://account.chargeplacescotland.org/api/v2/poi/chargepoint/dynamic`
        *   `https://account.chargeplacescotland.org/api/v3/poi/chargepoint/static`
    *   A public API key is used for authentication.
    *   The fetched data is saved as JSON files in the `data/source/` directory:
        *   `chargepoint_dynamic_data.json`
        *   `chargepoint_static_data.json`

*   **Monthly Session Reports Scraping:**
    *   The HTML content from `https://chargeplacescotland.org/monthly-charge-point-performance/` is scraped to find links to monthly session reports (in `.xlsx` or `.csv` format).
    *   For each found report, it downloads the file, converts it to CSV if necessary, and performs cleaning and standardization.
    *   Specific data cleaning and date format conversions are applied based on known issues in certain months/years.
    *   The cleaned monthly session data is saved as CSV files in the `downloaded_reports` directory and then consolidated into `data/source/all_sessions.csv`.

### 2. Dataset Generation Pipeline (`main.py`)

This script initiates the ChargePlaceScotlandAPI class (`src/chargeplace/chargeplace_scotland_api.py`), which performs the following sequence of operations:

1. **Load Data:** Loads the scraped session files, charge point information, and council area shapefiles.

2. **Create Directory Structure:** Creates a hierarchical folder structure in `data/result/`, with a directory for each local authority in Scotland.

3. **Geospatial Mapping:** Spatial joins used to map each charging point to its correct local authority based on latitude and longitude. The infrastructure data for each authority is saved as `charging_infrastructure.csv`.

4. **Session Processing:** This is the core of the pipeline. For each charger, it:

    - Determines the charger's entire operational period from its first to its last recorded session.

    - Fetches the corresponding carbon intensity and generation mix data for that period and location using the carbon module. API calls are cached to prevent redundant requests.

    - For each charging session, it estimates the energy consumed in 30-minute intervals. This assumes a uniform discharge rate based on the charger's nominal power.

    - It also flags each 30-minute interval as Occupied or not.

    - Finally, it merges the session data (energy consumed, occupancy) with the carbon data and saves the result as a unique CSV file (e.g., data/result/Glasgow City/sessions_mix/52117_1.csv).

## Dataset Output
The pipeline generates a structured dataset organized by local authority. Each authority's folder contains:

`charging_infrastructure.csv`: Static details for all chargers in that region (location, power, connector type, etc.).
 
`downloaded_reports/`: A directory containing individual CSV files for each month (e.g., `may-2025.csv`) containing all charging sessions and information about their start time, duration, price, etc.

`data/result/**/sessions_mix/`: A directory containing individual CSV files for each charger connector (e.g., `50656_1.csv`). Each file provides a 30-minute time-series of:

- Consumption.

- Occupancy status.

- A detailed breakdown of the electricity generation mix (gCO2/kWh from wind, solar, gas, etc.).

- The forecasted carbon intensity (Forecast).

`downloaded_reports/`: A directory containing individual CSV files for each month (e.g., `may-2025.csv`) containing all charging sessions and information about their start time, duration, price, etc.

`tariff_information/tariff.csv`: A file containing detailed tariff information of each charger. An additional file was generated by parsing the unstructured text from the Tariff Description column in the original `tariff.csv` file. This process was automated using a Large Language Model (Gemini 2 Flash), and extends the `tariff.csv` file to include detailed columns including overstay charge, minimum fee, flat rate, etc/


For a full description of the dataset schema, please refer to the [Hugging Face dataset card](https://huggingface.co/datasets/djordjebatic/GridCharge).

## License

This project is licensed under the CC-BY-4.0 License. See the Hugging Face dataset documentation for full licensing terms.

## Citation

If you use this code or the generated GridCharge dataset in your research, please cite:

```bibtex
@dataset{gridcharge2025,
    title  = {GridCharge: Capturing Carbon Intensity of 5M+ Public EV Charging Sessions in Scotland},
    author = {Djordje Batic},
    year   = {2025}
    url    = {https://huggingface.co/datasets/djordjebatic/GridCharge}
}
```

## Acknowledgments

- ChargePlace Scotland for providing charging session data
- UK National Grid ESO for carbon intensity data via their public API
- Scottish Government for geographic boundary data

## Contact

For questions about the code or dataset, please open an issue in this repository.