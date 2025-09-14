from datetime import timedelta
import pandas as pd
import pytz
import numpy as np


def process_session_data(timestamp, column, column_name, complete_data):
    """Create a timestamp-aligned DataFrame for a charger/session series.

    Keeps timezone-aware timestamps and fills missing intervals with zeros.
    """
    processed_data = pd.DataFrame({"timestamp": timestamp, column_name: column})
    processed_data = pd.merge(complete_data, processed_data, on="timestamp", how="left").fillna(0)

    processed_data["timestamp"] = pd.to_datetime(processed_data["timestamp"]).dt.tz_localize(pytz.utc)

    return processed_data


def calculate_time_intervals(start_time, charging_duration, real_charging_duration, granularity):
    """Return rounded start, end and stay times based on granularity (minutes).

    start_time : datetime
    charging_duration : timedelta-like (or pandas Timedelta)
    real_charging_duration : float (hours)
    granularity : int minutes
    """
    end_time = start_time + timedelta(minutes=60) * real_charging_duration
    stay_time = start_time + charging_duration

    rounded_start_time = start_time.replace(minute=(start_time.minute // granularity) * granularity, second=0,
                                            microsecond=0)
    rounded_end_time = end_time.replace(minute=(end_time.minute // granularity) * granularity, second=0,
                                        microsecond=0) + timedelta(minutes=granularity)

    rounded_stay_time = stay_time.replace(minute=(stay_time.minute // granularity) * granularity, second=0,
                                          microsecond=0) + timedelta(minutes=granularity)

    return rounded_start_time, rounded_end_time, rounded_stay_time


def get_time_energy(start_time, num_intervals, granularity, max_charge_rate, total_consumed):
    """Distribute energy across rounded intervals and return time series + energy per interval.

    Returns (time_series, energy_series)
    """
    time_series = []
    energy_series = []
    accumulated_energy = 0.0

    for i in range(num_intervals):
        charging_granularity = timedelta(minutes=granularity)

        interval_time = start_time + timedelta(minutes=granularity * i)
        rounded_time = interval_time.replace(minute=(interval_time.minute // granularity) * granularity, second=0,
                                             microsecond=0)
        time_series.append(rounded_time)

        if i == 0:
            current_charging_period = charging_granularity - (interval_time - rounded_time)
        else:
            current_charging_period = charging_granularity

        # energy charged this period (kWh)
        current_period_charged = (max_charge_rate / (60 / granularity)) * current_charging_period / charging_granularity

        accumulated_energy += current_period_charged

        if i == num_intervals - 1:
            # correct final period so total matches exactly
            current_period_charged = current_period_charged - (accumulated_energy - total_consumed)

        energy_series.append(current_period_charged)

    return time_series, energy_series


def get_time_occupied(start_time, num_intervals, granularity):
    time_series = []
    occupied_series = []
    for i in range(num_intervals):
        interval_time = start_time + timedelta(minutes=granularity * i)
        rounded_time = interval_time.replace(minute=(interval_time.minute // granularity) * granularity, second=0,
                                             microsecond=0)
        time_series.append(rounded_time)
        occupied_series.append(1)

    return time_series, occupied_series
