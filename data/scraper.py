import requests
import json
import os
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd
import io 
import numpy as np
from datetime import timedelta


def clean_data(df):
    # Handle missing values: fill with NaN and optionally drop or fill them

    df.replace('', np.nan, inplace=True)
    df.dropna(subset=['Duration', 'Start', 'Consumed(kWh)'], inplace=True)  # Drop rows where elements are NaN
    # df.fillna(method='ffill', inplace=True)  # Forward fill NaN values
    
    # drop rows with zero consumption
    df = df[df['Consumed(kWh)'] > 0]

    # Remove duplicates
    # df.drop_duplicates(inplace=True)

    # Function to check if a string represents a valid timedelta (not a date)
    def is_valid_timedelta(value):
        try:
            pd.to_timedelta(value)
            return True
        except Exception as e:
            # print(f"  -> ERROR: Reason: {e}")
            return False

    def is_valid_datetime(value):
        try:
            pd.to_datetime(value, format='%Y-%m-%d %H:%M:%S', errors='raise')
            return True
        except Exception as e:
            # print(f"  -> ERROR: Reason: {e}")
            return False

    # Apply the function to create a boolean mask
    mask_1 = df['Duration'].apply(is_valid_timedelta)
    mask_2 = df['Start'].apply(is_valid_datetime)

    # Filter the DataFrame to keep only valid timedelta strings
    df_filtered = df[mask_1 & mask_2]

    print(f'  -> Cleaning the data: Removed {round((len(df) - len(df_filtered)) / len(df), 6) * 100}% of records')
    #else:
    #    df_filtered[column] = df_filtered[column].astype(dtype, errors='ignore')

    df_filtered = df_filtered.dropna(subset=['Duration', 'Start', 'Consumed(kWh)'])  # Drop rows where elements are NaN

    # Remove any rows where Duration is longer than 1 day
    try:
        durations_td = pd.to_timedelta(df_filtered['Duration'], errors='coerce')
        mask_duration = durations_td <= pd.Timedelta(days=1)
        removed_long = (~mask_duration).sum()
        if removed_long:
            print(f"  -> Filtering: Removed {int(removed_long)} record(s) with 'Duration' > 1 day")
        df_filtered = df_filtered[mask_duration]
    except Exception as e:
        print(f"  -> Warning: Failed to filter long durations. Reason: {e}")

    return df_filtered


def convert_datetime_with_validation(df, column_name, known_year, known_month):
    """
    Identifies the correct datetime format by validating against the first and last records.
    """
    if df[column_name].dropna().empty:
        print("Column is empty or all NaN. No conversion performed.")
        return df

    # Get the first and last valid string values from the column
    mid_val = df[column_name].dropna().iloc[int(len(df) / 2)]

    possible_month_formats = [
        '%d-%m-%Y %H:%M:%S', '%d/%m/%Y %H:%M:%S', '%d-%m-%y %H:%M:%S',
        '%m-%d-%Y %H:%M:%S', '%m/%d/%Y %H:%M:%S', '%m-%d-%y %H:%M:%S',
        '%d/%m/%Y %H:%M', '%d-%m-%Y %H:%M',
        '%m/%d/%Y %H:%M', '%m-%d-%Y %H:%M',
        '%d/%m/%Y', '%d-%m-%Y',
        '%m/%d/%Y', '%m-%d-%Y'
    ]

    possible_year_formats = [
        '%Y-%m-%d %H:%M:%S', '%Y/%m/%d %H:%M:%S',
    ]

    candidate_formats = []
    for fmt in possible_month_formats:
        try:
            parsed_date = pd.to_datetime(mid_val, format=fmt)
            if parsed_date.year == known_year and parsed_date.month == known_month:
                candidate_formats.append(fmt)
        except (ValueError, TypeError):
            continue

    correct_format = None
    if len(candidate_formats) == 1:
        correct_format = candidate_formats[0]
        print(f"  -> Unambiguous format found: '{correct_format}'")
    elif len(candidate_formats) == 2:
        for fmt in candidate_formats:
            try:
                parsed_date = pd.to_datetime(mid_val, format=fmt)
                parsed_date = parsed_date + timedelta(hours=12)
                if parsed_date.year == known_year and parsed_date.month == known_month:
                    correct_format = fmt
                    print(f"  -> Validation successful. Correct format is: '{correct_format}'")
                    break
            except (ValueError, TypeError):
                continue

    else:
        print(f"  -> Ambiguous format detected. Candidates: {candidate_formats}. Validating with first and last record...")
        for fmt in possible_year_formats:
            try:
                parsed_date = pd.to_datetime(mid_val, format=fmt)
                if parsed_date.year == known_year and parsed_date.month == known_month:
                    correct_format = fmt
                    print(f"  -> Validation successful. Correct format is: '{correct_format}'")
                    break
            except (ValueError, TypeError):
                continue

    if not correct_format:
        raise ValueError("  -> Could not resolve ambiguity. None of the candidate formats result in a valid chronological order.")

    df[column_name] = pd.to_datetime(df[column_name], 
                                    format=correct_format, 
                                    errors='coerce')
    print(f"  -> Column '{column_name}' successfully converted using format '{correct_format}'.")
    return df


def map_and_filter_columns(df):
    """
    Renames DataFrame columns to a standard format and filters for desired columns.

    Args:
        df (pd.DataFrame): The input DataFrame with inconsistent column names.

    Returns:
        pd.DataFrame: A DataFrame with standardized and filtered columns.
    """
    rename_map = {}
    for col in df.columns:
        # Normalize column name for robust matching (lowercase, no extra spaces)
        col_norm = str(col).lower().strip()
        
        if 'cp' in col_norm and 'id' in col_norm or 'display id' in col_norm:
            rename_map[col] = 'CP ID'
        # Updated Logic: More robustly target connector ID, including typos like 'Connecto'
        elif (('connector' in col_norm or 'connecto' in col_norm) and 'id' in col_norm) or col_norm == 'connector':
            rename_map[col] = 'Connector'
        #elif 'curr' in col_norm:
        #    rename_map[col] = 'Currency'
        elif 'amount' in col_norm or 'amt' in col_norm:
            rename_map[col] = 'Paid(gbp)'
        elif 'consum' in col_norm:
            rename_map[col] = 'Consumed(kWh)'
        elif 'duration' in col_norm:
            rename_map[col] = 'Duration'
        elif 'start' in col_norm:
            rename_map[col] = 'Start'

    df.rename(columns=rename_map, inplace=True)

    # Define the final, ordered list of columns we want to keep
    desired_columns = ['Start', 'Duration', 'Consumed(kWh)', 'Paid(gbp)', 'CP ID', 'Connector']

    # Filter the DataFrame to only keep desired columns that are present after renaming
    present_columns = [col for col in desired_columns if col in df.columns]
    
    df = df.assign(
        **{
            'CP ID': df['CP ID'].astype(str).str.strip(),
            'Connector': pd.to_numeric(df['Connector'], downcast='integer', errors='coerce'),
            'Consumed(kWh)': pd.to_numeric(df['Consumed(kWh)'], downcast='float', errors='coerce'),
            'Paid(gbp)': pd.to_numeric(df['Paid(gbp)'], downcast='float', errors='coerce'),
            'Duration': df['Duration'].astype(str).str.strip(),
            'Start': df['Start'].astype(str).str.strip()
        }
    )
    
    return df[present_columns]

def scrape_sessions_data(html_data):
    """
    Parses HTML to find and download monthly session reports, converting all to CSV.

    Args:
        html_data (str): The HTML content as a string.
    """
    base_url = "https://chargeplacescotland.org/"
    output_dir = "downloaded_reports"
    os.makedirs(output_dir, exist_ok=True)
    print(f"Files will be saved as CSV in the '{output_dir}' directory.")

    soup = BeautifulSoup(html_data, 'html.parser')
    session_links = soup.find_all(
        'a',
        text=re.compile(r'session', re.I),
        href=re.compile(r'\.(xlsx|csv)$', re.I)
    )

    if not session_links:
        print("No session report links found.")
        return

    print(f"Found {len(session_links)} potential session files to download and convert.\n")

    import calendar
                
    month_map = {m.lower(): i for i, m in enumerate(calendar.month_name) if i}

    all_sessions = []
    for link in session_links:
        file_url = link.get('href')
        original_ext = os.path.splitext(file_url)[1].lower()
        
        month = None
        year = None
        # Start searching from the link's parent paragraph for better context
        current_element = link.find_parent('p')
        if not current_element:
            continue

        # Search the text of the paragraph itself first
        search_text_initial = current_element.get_text()
        match_initial = re.search(r'([A-Za-z]+)\s+(20\d{2})', search_text_initial)
        if match_initial:
            month = match_initial.group(1).lower()
            year = match_initial.group(2)
        else:
            # If not found, then search previous siblings as a fallback
            for sibling in current_element.find_previous_siblings(limit=10):
                search_text_sibling = sibling.get_text()
                match_sibling = re.search(r'([A-Za-z]+)\s+(20\d{2})', search_text_sibling)
                if match_sibling:
                    month = match_sibling.group(1).lower()
                    year = match_sibling.group(2)
                    break

        if not month or not year:
            print(f"Could not find a date for: {file_url}. Skipping.")
            continue

        new_filename = f"{month}-{year}.csv"
        output_filepath = os.path.join(output_dir, new_filename)
        
        download_url = urljoin(base_url, file_url)

        print(f"Processing report for: {month.capitalize()} {year}")
        print(f"  -> Original URL: {file_url}")
        print(f"  -> Saving as: {new_filename}")

        #if (month.lower() not in 'october') or int(year) != 2024:
        #    continue

        try:
            response = requests.get(download_url, timeout=15)
            response.raise_for_status()
            file_content = io.BytesIO(response.content) # Use BytesIO for in-memory file handling

            if original_ext == '.xlsx':
                # Read from excel file in memory
                df = pd.read_excel(file_content, engine='openpyxl')
                message = "Converted from XLSX to CSV and saved."
            else: # The file is a CSV
                # Read from csv file in memory, trying common encodings
                try:
                    df = pd.read_csv(file_content, encoding='utf-8')
                except UnicodeDecodeError:
                    file_content.seek(0) # Reset buffer position
                    df = pd.read_csv(file_content, encoding='latin1') # Fallback encoding
                message = "CSV file downloaded successfully."
            
            # Print the header of the cleaned DataFrame
            print(f"  -> Old Header: {list(df.columns)}")

            # Standardize the column headers
            df = map_and_filter_columns(df)

            df.dropna(subset=['Start'], inplace=True)


            # Print the header of the cleaned DataFrame
            print(f"  -> New Header: {list(df.columns)}")

            # Convert string to its number; use .lower() for case-insensitivity
            month_number = month_map[month.lower()]

            # print(f"  -> Types: {df.info()}")

            print(f"  -> df['Start']: {df['Start'].iloc[100]}, Month: {month_number}, Year: {year} ")
            # print(f"  -> df['Duration']: {df['Duration'].iloc[100]}")

            # Handle known data issues

            # Specific fix for known 'T' issue in October 2023 data
            if (int(year) == 2023 and month_number == 10):
                df['Start'] = df['Start'].str.replace('T', ' ')

            # Specific fix for known seconds issue in Duration for September 2024 data
            if int(year) == 2024 and month_number == 9:

                def seconds_to_hh_mm_ss(seconds):
                    # Convert seconds to timedelta
                    td = pd.Timedelta(seconds=seconds)
                    # Extract hours, minutes, seconds and format as hh:mm:ss
                    hours, remainder = divmod(td.seconds, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

                df.dropna(subset=['Duration'], inplace=True)
                df['Duration'] = df['Duration'].apply(seconds_to_hh_mm_ss)


            # Convert column to datetime objects, coercing errors to NaT (Not a Time)
            if df['Start'].dtype != 'datetime64[ns]':
                df = convert_datetime_with_validation(
                    df=df,
                    column_name='Start',
                    known_year=int(year),
                    known_month=month_number
                )

            df.dropna(subset=['Start'], inplace=True)
            df['Start'] = df['Start'].dt.strftime('%Y-%m-%d %H:%M:%S')
            df = df.sort_values('Start').reset_index(drop=True)

            df_clean = clean_data(df)
            
            # Save the DataFrame to a CSV file
            df_clean.to_csv(output_filepath, index=False, encoding='utf-8')
            
            all_sessions.append(df_clean)

            print(f"  -> SUCCESS: {message} ")
        
        except Exception as e: # Catch pandas and other errors
            print(f"  -> ERROR: Could not process file. \n      Reason: {e}")
        
        print("-" * 30)

    out = pd.concat(all_sessions, ignore_index=True)
    out.to_csv('data/source/all_sessions.csv', index=False)
    print(f"Successfully saved all sessions to 'data/source/all_sessions.csv'.")


def scrape_chargepoint_data(api_url, api_key, output_file_name):
    """
    Scrapes data from a given API URL, saves it to a JSON file, 
    and prints the number of stations and chargers.

    Args:
        api_url (str): The URL of the API endpoint.
        api_key (str): The API authentication key.
        output_file_name (str): The name of the file to save the data to.
    """
    try:
        # Send the GET request with the API authentication header
        response = requests.get(api_url, headers={'api-auth': api_key})

        # Raise an exception for bad status codes (4xx or 5xx)
        response.raise_for_status()

        # Parse the JSON response
        data = response.json()

        # Save the data to a JSON file with pretty-printing
        with open(output_file_name, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        
        print(f"Successfully saved data to {output_file_name}")

        if output_file_name == "chargepoint_dynamic_data.json":

            # Count stations and chargers
            charge_points = data.get('chargePoints', [])
            num_stations = len(charge_points)
            num_chargers = sum(len(cp.get('chargePoint', {}).get('connectorGroups', [])) for cp in charge_points)

            print(f"Observed {num_stations} total stations and {num_chargers} total chargers.")

    except requests.exceptions.HTTPError as errh:
        print(f"Http Error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        print(f"Error Connecting: {errc}")
    except requests.exceptions.Timeout as errt:
        print(f"Timeout Error: {errt}")
    except requests.exceptions.RequestException as err:
        print(f"An unexpected error occurred: {err}")
    except json.JSONDecodeError:
        print("Failed to decode JSON from the response.")


if __name__ == "__main__":
    
    
    # --- CHARGEPOINT API SCRAPER ---
    # Define API endpoints and key
    api_url_chargepoint_collection = "https://account.chargeplacescotland.org/api/v2/poi/chargepoint/dynamic"
    api_url_feature_collection = "https://account.chargeplacescotland.org/api/v3/poi/chargepoint/static"
    api_key = "c3VwcG9ydCtjcHNhcHBAdmVyc2FudHVzLmNvLnVrOmt5YlRYJkZPJCEzcVBOJHlhMVgj"
    
    # Specify output file names
    output_file_dynamic = "data/source/chargepoint_dynamic_data.json"
    output_file_static = "data/source/chargepoint_static_data.json"

    # Call the function for both endpoints
    print("Scraping dynamic chargepoint data...")
    scrape_chargepoint_data(api_url_chargepoint_collection, api_key, output_file_dynamic)
    
    print("\nScraping static chargepoint data...")
    scrape_chargepoint_data(api_url_feature_collection, api_key, output_file_static)
    

    # --- SESSIONS SCRAPER ---
    print("\nScraping monthly session reports...")
    url = 'https://chargeplacescotland.org/monthly-charge-point-performance/'
    try:
        r = requests.get(url)
        r.raise_for_status()
        html_content = r.text
        scrape_sessions_data(html_content)
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch the webpage for session reports: {e}")

