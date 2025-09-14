import json
import logging
from datetime import datetime, timedelta, date
from time import sleep
import requests
import pandas as pd
from numpy import nan
from shapely.geometry import Point
import geopandas as gpd
import os
import numpy as np
from src.carbon.carbon_intensity_api import CarbonIntensityAPI
from src.carbon.carbon_adapter import CarbonAdapter
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.chargeplace.sessions import process_session_data, calculate_time_intervals, get_time_energy, get_time_occupied
import pytz

# basic logging for visibility
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



class ChargePlaceScotlandAPI:
    """
    Interface with the ChargePlaceScotland Data.

    Parameters
    ----------
    `retries` : int
        Optionally specify the number of retries to use should the API respond with anything
        other than status code 200. Exponential back-off applies inbetween retries.
    """

    def __init__(self,
                 feature_collection_path,
                 sessions_path,
                 council_areas_polygon_path,
                 council_areas_path):

        self.feature_collection = self._parse_features_json_to_df(feature_collection_path)
        self.feature_collection = self._create_gdf_instance(self.feature_collection)
        self.sessions = pd.read_csv(sessions_path)
        #self.sessions = self.sessions.drop([], axis=1)

        self.sessions['Duration'] = pd.to_timedelta(self.sessions['Duration'])
        self.sessions['Start'] = pd.to_datetime(self.sessions['Start'])
        self.sessions['charging_period'] = self.sessions['Duration'].dt.total_seconds() / 3600
        self.sessions['CP ID'] = self.sessions['CP ID'].astype(str)
        self.sessions['Connector'] = pd.to_numeric(self.sessions['Connector'], downcast='integer', errors='coerce')
        self.sessions['Consumed(kWh)'] = pd.to_numeric(self.sessions['Consumed(kWh)'], downcast='float', errors='coerce')
        self.sessions['Paid(gbp)'] = pd.to_numeric(self.sessions['Paid(gbp)'], downcast='float', errors='coerce')

        self.sessions = self.sessions[['Start', 'Duration', 'Consumed(kWh)', 'Paid(gbp)', 'CP ID', 'Connector']]

        # self.sessions = self.sessions[:100000]

        #self.sessions = pd.merge(self.sessions, self.feature_collection[['name', 'id', 'capacity']], left_on=['CP ID', 'Connector'], right_on=['name', 'id'])
        #self.sessions['real_charging_duration'] = self.sessions['Consum(kWh)'] / self.sessions['connectorMaxChargeRate']

        # self.sessions.dropna(subset=['Duration', 'Start'])

        council_areas_polygon = gpd.read_file(council_areas_polygon_path)
        council_areas_regions = pd.read_csv(council_areas_path)

        self.council_areas = pd.merge(council_areas_polygon, council_areas_regions, on='local_auth')
        self.council_areas = self.council_areas.drop(['la_s_code',
                                                      'cc_name',
                                                      'active',
                                                      'url',
                                                      'sh_date_up',
                                                      'sh_src',
                                                      'sh_src_id'], axis=1)

        # Carbon adapter with caching; reuse across calls
        self.carbon_adapter = CarbonAdapter()

    def create_folder_structure(self, base_dir='data/result'):
        local_authorities = self.council_areas['local_auth'].unique()
        for local_auth in local_authorities:
            directory_path = os.path.join(base_dir, local_auth, 'sessions_mix')
            if not os.path.exists(directory_path):
                try:
                    os.makedirs(directory_path, exist_ok=True)
                    logger.info("Directory '%s' created successfully.", local_auth)
                except OSError as error:
                    logger.error("Error creating directory '%s': %s", local_auth, error)
            else:
                logger.debug("Directory '%s' already exists.", local_auth)

    def locate_council_area_charging_infrastructure(self, base_dir='data/result'):
        def remove_incode(postcode):
            if ' ' in postcode:
                return postcode.split(' ')[0]
            else:
                return postcode[:-3]

        for local_auth in os.listdir(base_dir):
            if os.path.isdir(os.path.join(base_dir, local_auth)):
                council_area = self.council_areas[self.council_areas['local_auth'] == local_auth]
                # council_area = council_area[['geometry']]

                if council_area.crs != self.feature_collection.crs:
                    council_area = council_area.to_crs(self.feature_collection.crs)

                gdf = gpd.sjoin(self.feature_collection, council_area, how='inner', op='within')
                gdf = gdf.reset_index(drop=True)

                gdf['Postcode'] = gdf['Postcode'].apply(remove_incode)

                gdf = gdf.drop(['index_right', 'geometry'], axis=1)
                
                gdf = gdf.assign(
                    **{
                        'Local Authority': gdf['local_auth'].astype(str).str.strip(),
                        'Region ID': gdf['region_id'].astype(str).str.strip(),
                    }
                )
                gdf = gdf[['Latitude', 'Longitude', 'CP ID', 'Connector', 
                           'Nominal Power (kW)', 'Connector Type', 'Tariff', 'Connection Fee',
                           'Address', 'Postcode', 'Local Authority', 'Region ID']]
                
                gdf_path = os.path.join(base_dir, local_auth, 'charging_infrastructure.csv')
                gdf.to_csv(gdf_path, index=False)

    def populate_session_data_per_charger(self, granularity=30, base_dir='data/result', max_workers=4):
        """Process each local authority in parallel (bounded by max_workers)."""
        local_auths = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]

        def _process(local_auth):
            infra_path = os.path.join(base_dir, local_auth, 'charging_infrastructure.csv')
            if not os.path.exists(infra_path):
                logger.warning("No charging_infrastructure.csv for %s, skipping", local_auth)
                return
            charging_infrastructure = pd.read_csv(infra_path)
            self.generate_charging_data_with_rounded_time(charging_infrastructure, granularity,
                                                          folder=os.path.join(base_dir, local_auth, 'sessions_mix'))

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(_process, la): la for la in local_auths}
            for fut in as_completed(futures):
                la = futures[fut]
                try:
                    fut.result()
                except Exception as e:
                    logger.exception("Error processing %s: %s", la, e)
                    

    def generate_charging_data_with_rounded_time(self, df, granularity, folder):

        df['Connector'] = df['Connector'].astype(str)
        df['CP ID'] = df['CP ID'].astype(str)
        self.sessions['CP ID'] = self.sessions['CP ID'].astype(str)
        self.sessions['Connector'] = self.sessions['Connector'].astype(str)
        session_df = pd.merge(self.sessions, df, on=['CP ID', 'Connector'], how='inner')

        ######

        start_times = session_df['Start']

        end_times = start_times + session_df['Duration']

        overall_start_time = start_times.min().replace(minute=(start_times.min().minute // granularity) * granularity,
                                                       second=0,
                                                       microsecond=0)
        end_minute = end_times.max().minute // granularity

        overall_end_time = end_times.max().replace(minute=(end_times.max().minute // granularity) * granularity,
                                                   second=0,
                                                   microsecond=0)
        overall_end_time = overall_end_time + timedelta(minutes=granularity)

        auth = session_df['Local Authority'].mode()[0]
        code = session_df['Postcode'].mode()[0]
        logger.info('Processing %s %s...', auth, code)

        # get carbon intensity data for this postcode / region (cached)
        start_iso = overall_start_time.isoformat()
        end_iso = overall_end_time.isoformat()
        carbon_data, gen_mix_data = self.carbon_adapter.fetch(start_iso, end_iso, "postcode", postcode=session_df['Postcode'].mode()[0])

        ######

        grouped = session_df.groupby(['CP ID', 'Connector'])

        for (cp_id, connector), group in grouped:

            start_times = group['Start']

            end_times = start_times + pd.to_timedelta(group['Duration'], unit='h')

            overall_start_time = start_times.min().replace(
                minute=(start_times.min().minute // granularity) * granularity, second=0,
                microsecond=0)

            overall_end_time = end_times.max().replace(minute=(end_times.max().minute // granularity) * granularity,
                                                        second=0,
                                                        microsecond=0)
            overall_end_time = overall_end_time + timedelta(minutes=granularity)

            if len(group['Postcode'].unique()) > 1:
                raise ValueError()

            # Generate a complete time series for the overall operational period
            complete_time_series = pd.date_range(start=overall_start_time, end=overall_end_time, freq=f'{granularity}T')

            charging_time_series_all = []
            energy_series_all = []

            occupied_time_series_all = []
            occupied_series_all = []

            for index, row in group.iterrows():
                cp_id = row['CP ID']
                connector = row['Connector']
                start_time = row['Start']
                max_charge_rate = row['Nominal Power (kW)']
                total_consumed = row['Consumed(kWh)']
                postcode = row['Postcode']
                region_id = row['Region ID']
                local_auth = row['Local Authority']
                charging_duration = row['Duration']

                real_charging_duration = total_consumed / max_charge_rate  # in hours

                rounded_start, rounded_end, rounded_stay = self.calculate_time_intervals(start_time,
                                                                                            charging_duration,
                                                                                            real_charging_duration,
                                                                                            granularity)

                # Calculate number of real charging intervals (n minutes each)
                num_charging_intervals = int((rounded_end - rounded_start) / timedelta(minutes=granularity))
                charging_time_series, energy_series = self.get_time_energy(start_time,
                                                                            num_charging_intervals,
                                                                            granularity,
                                                                            max_charge_rate,
                                                                            total_consumed)

                difference = round(np.sum(energy_series), 2) - round(total_consumed, 2)
                assert difference < 0.1, f'Energy mismatch of {difference} for {cp_id}_{connector} at {start_time}'

                # Calculate number of charging intervals with overstay(n minutes each)
                try:
                    num_stay_intervals = int((rounded_stay - rounded_start) / timedelta(minutes=granularity))
                except:
                    print(f'Issue stay period {cp_id}_{connector} at {start_time}')
                    continue

                occupied_time_series, occupied_series = self.get_time_occupied(start_time, num_stay_intervals,
                                                                                granularity)

                # save both
                charging_time_series_all.extend(charging_time_series)
                energy_series_all.extend(energy_series)

                occupied_time_series_all.extend(occupied_time_series)
                occupied_series_all.extend(occupied_series)

            complete_data = pd.DataFrame({'timestamp': complete_time_series})

            # Create DataFrame for the charger
            processed_charging_data = self.process_session_data(timestamp=charging_time_series_all,
                                                                column=energy_series_all,
                                                                column_name='consumed_total',
                                                                complete_data=complete_data)

            #
            processed_occupied_data = self.process_session_data(timestamp=occupied_time_series_all,
                                                                column=occupied_series_all,
                                                                column_name='occupied',
                                                                complete_data=complete_data)

            processed_session_data = processed_charging_data.merge(processed_occupied_data, on='timestamp')

            merged_df = processed_session_data.merge(carbon_data, on='timestamp').merge(gen_mix_data, on='timestamp')

            for column in ['biomass', 'coal', 'imports', 'gas', 'nuclear', 'other', 'hydro', 'solar', 'wind']:
                merged_df[column] = merged_df[column] / 100

            if len(merged_df['regionid_x'].mode()) != 0:
                if region_id != merged_df['regionid_x'].mode()[0]:
                    logger.warning('Local auth %s is set to %s but should be %s', local_auth, region_id, merged_df['regionid_x'].mode()[0])
            else:
                logger.warning('Problematic station without Region ID: %s_%s', cp_id, connector)

            merged_df['Region ID'] = region_id
            merged_df = merged_df.drop(['regionid_x', 'regionid_y'], axis=1)

            # Reorganizing columns using reindex
            new_column_order = ['Timestamp',
                                'Consumed',
                                'Occupied',
                                'Biomass', 
                                'Coal', 
                                'Gas', 
                                'Nuclear', 
                                'Hydro', 
                                'Solar', 
                                'Wind',
                                'Imports', 
                                'Other', 
                                'Forecast',
                                'Carbon Index',
                                'Region ID']
            old_columns = [
                                'timestamp',
                                'consumed_total',
                                'occupied',
                                'biomass', 
                                'coal', 
                                'gas', 
                                'nuclear', 
                                'hydro', 
                                'solar', 
                                'wind',
                                'imports',
                                'other',  
                                'forecast',
                                'index',
                                'region_id'
            ]   

            # Rename columns from old_columns to new_column_order (mapping by position)
            rename_map = dict(zip(old_columns, new_column_order))
            merged_df = merged_df.rename(columns=rename_map)

            merged_df = merged_df.reindex(columns=new_column_order)

            # Save to CSV
            filename = f"{cp_id}_{connector}.csv"
            filename = os.path.join(folder, filename)
            merged_df.to_csv(filename, index=False)

    def process_session_data(self, timestamp, column, column_name, complete_data):
        return process_session_data(timestamp, column, column_name, complete_data)

    def calculate_time_intervals(self, start_time, charging_duration, real_charging_duration, granularity):
        return calculate_time_intervals(start_time, charging_duration, real_charging_duration, granularity)

    def get_time_energy(self, start_time, num_intervals, granularity, max_charge_rate, total_consumed):
        return get_time_energy(start_time, num_intervals, granularity, max_charge_rate, total_consumed)

    def get_time_occupied(self, start_time, num_intervals, granularity):
        return get_time_occupied(start_time, num_intervals, granularity)

    def get_generation_mix(self, df):
        pass

    def _create_gdf_instance(self, df, epsg=4326):
        df['Latitude'] = df['Latitude'].astype(float)
        df['Longitude'] = df['Longitude'].astype(float)

        df['geometry'] = [Point(x, y) for x, y in zip(df['Longitude'], df['Latitude'])]

        gdf = gpd.GeoDataFrame(df, geometry=df['geometry'])
        # set_crs returns None when inplace=True, so call without assignment
        gdf.set_crs(epsg=epsg, inplace=True)
        return gdf

    def _parse_features_json_to_df(self, location):

        with open(location) as f:
            json_data = json.load(f)

        rows = []

        for feature in json_data['features']:
            latitude, longitude = feature['geometry']['coordinates']
            for group in feature['properties']['connectorGroups']:
                group_id = group['connectorGroupID']
                for connector in group['connectors']:
                    row = {
                    'Latitude': latitude,
                    'Longitude': longitude,
                    'CP ID': feature['properties']['name'],
                    'Connector': group_id,
                    'Tariff': feature['properties']['tariff']['amount'],
                    'Connection Fee': feature['properties']['tariff']['connectionfee'],
                    'Address': feature['properties']['address']['sitename'],
                    'Postcode': feature['properties']['address']['postcode'],
                    'Connector Type': connector['connectorPlugTypeName'],
                    'Nominal Power (kW)': connector['connectorMaxChargeRate']
                    }
                    rows.append(row)

        dataset = pd.DataFrame(rows)

        # Normalize types: keep id/name as str, numeric where appropriate
        if not dataset.empty:
            dataset['Latitude'] = pd.to_numeric(dataset['Latitude'], errors='coerce')
            dataset['Longitude'] = pd.to_numeric(dataset['Longitude'], errors='coerce')
            # Ensure is numeric (max charge rate)
            dataset['Nominal Power (kW)'] = pd.to_numeric(dataset['Nominal Power (kW)'], downcast='float', errors='coerce')
            dataset['Tariff'] = pd.to_numeric(dataset['Tariff'], downcast='float', errors='coerce')
            dataset['Connection Fee'] = pd.to_numeric(dataset['Connection Fee'], downcast='float', errors='coerce')
            # Ensure is string (id/name/postcode)
            dataset['Connector'] = dataset['Connector'].astype(str)
            dataset['Connector Type'] = dataset['Connector Type'].astype(str)
            dataset['CP ID'] = dataset['CP ID'].astype(str)
            dataset['Postcode'] = dataset['Postcode'].astype(str)
            dataset['Address'] = dataset['Address'].astype(str)

        return dataset

    def between(self, start, end, type="national", region_id=None):
        """
        Get the Carbon Intensity forecast and actual results for a given time interval from the API.

        Parameters
        ----------
        `start` : datetime
            A timezone-aware datetime object. Will be corrected to the END of the half hour in which
            *start* falls, since ESO use end of interval as convention.
        `end` : datetime
            A timezone-aware datetime object. Will be corrected to the END of the half hour in which
            *end* falls, since ESO use end of interval as convention.
        `type` : str
            Either "national" or "regional".
        Returns
        -------
        Pandas DataFrame
            Carbon intensity data for the requested period.
        Notes
        -----
        For list of optional *extra_fields*, see `PV_Live API Docs
        <https://www.solar.sheffield.ac.uk/pvlive/api/>`_.
        """
        type_check = not (isinstance(start, datetime) and isinstance(end, datetime))
        tz_check = start.tzinfo is None or end.tzinfo is None
        if type_check or tz_check:
            raise Exception("Start and end must be timezone-aware Python datetime objects.")
        start = self._nearest_hh(start)
        end = self._nearest_hh(end)
        type = type.lower()
        if type not in ("national", "regional"):
            raise Exception("Type must be either 'national' or 'regional'.")
        endpoint = "/regional/intensity/{}/{}" if type == "regional" else "/intensity/{}/{}"
        region_suffix = f"/regionid/{str(region_id)}" if type == "regional" else ""
        carbon = None
        gen_mix = None
        request_start = start
        max_range = self.max_range[type]
        while request_start < end:
            request_end = min(end, request_start + self.max_range[type] - timedelta(minutes=30))
            request_endpoint = endpoint.format(request_start.isoformat(), request_end.isoformat())
            url = f"{self.base_url}{request_endpoint}{region_suffix}"
            response = self.query_api(url)
            data = self._parse_fromto_json(response, type)
            carbon_ = data if type == "national" else data[0]
            gen_mix_ = None if type == "national" else data[1]
            if carbon is None:
                carbon = carbon_
            else:
                carbon = pd.concat((carbon, carbon_), ignore_index=True)
            if gen_mix is None:
                gen_mix = gen_mix_
            else:
                gen_mix = pd.concat((gen_mix, gen_mix_), ignore_index=True)
            request_start += self.max_range[type]
        return carbon, gen_mix

    def _parse_fromto_json(self, response, type):
        """Parse the response from the /{from}/{to} endpoints into Pandas DataFrame."""
        if type == "national":
            data_list = [
                [d["to"], d["intensity"].get("forecast", nan), d["intensity"].get("actual", nan),
                 d["intensity"]["index"]] for d in response["data"]
            ]
            data = pd.DataFrame(data_list, columns=["timestamp", "forecast", "actual", "index"])
            data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True, infer_datetime_format=True)
            return data
        else:
            carbon_list = []
            fuel_mix_labels = ["biomass", "coal", "imports", "gas", "nuclear", "other", "hydro",
                               "solar", "wind"]
            fuel_mix_list = []

            region = response["data"]
            for datum in region["data"]:
                carbon_list.append([datum["to"], region["regionid"],
                                    datum["intensity"].get("forecast", nan),
                                    datum["intensity"].get("actual", nan),
                                    datum["intensity"]["index"]])
                fuel_mix = {f["fuel"]: f["perc"] for f in datum["generationmix"]}
                fuel_mix_list.append([datum["to"], region["regionid"]] +
                                     [fuel_mix[l] for l in fuel_mix_labels])
            carbon_data = pd.DataFrame(carbon_list, columns=["timestamp", "regionid", "forecast",
                                                             "actual", "index"])
            carbon_data["timestamp"] = pd.to_datetime(carbon_data["timestamp"], utc=True,
                                                      infer_datetime_format=True)
            fuel_mix_data = pd.DataFrame(fuel_mix_list,
                                         columns=["timestamp", "regionid"] + fuel_mix_labels)
            fuel_mix_data["timestamp"] = pd.to_datetime(fuel_mix_data["timestamp"], utc=True,
                                                        infer_datetime_format=True)
            return carbon_data, fuel_mix_data

    def query_api(self, url):
        """Query the API."""
        return self._fetch_url(url)

    def _fetch_url(self, url):
        """Fetch the URL with GET request."""
        success = False
        try_counter = 0
        delay = 1
        while not success and try_counter < self.retries + 1:
            try_counter += 1
            try:
                page = requests.get(url, params={}, headers=self.headers)
                page.raise_for_status()
                success = True
            except requests.exceptions.HTTPError:
                sleep(delay)
                delay *= 2
                continue
            except:
                raise
        if not success:
            raise Exception("Error communicating with the Carbon Intensity API.")
        try:
            return page.json()
        except:
            raise Exception("Error communicating with the Carbon Intensity API.")

    def _nearest_hh(self, dt):
        """Round a given datetime object up to the nearest hafl hour."""
        if not (dt.minute % 30 == 0 and dt.second == 0 and dt.microsecond == 0):
            dt = dt - timedelta(minutes=dt.minute % 30, seconds=dt.second) + timedelta(minutes=30)
        return dt

