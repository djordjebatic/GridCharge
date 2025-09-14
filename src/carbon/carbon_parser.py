import pandas as pd
from numpy import nan


class CarbonParser:
    """Parse Carbon Intensity API JSON payloads into pandas DataFrames.

    This is kept deterministic and side-effect free so it is easy to unit test.
    """

    FUEL_MIX_LABELS = ["biomass", "coal", "imports", "gas", "nuclear", "other", "hydro", "solar", "wind"]

    def parse_fromto_json(self, response, type_):
        """Parse the response from the /{from}/{to} endpoints into DataFrame(s).

        Returns:
            If type_ == 'national': (carbon_df, None)
            else: (carbon_df, fuel_mix_df)
        """
        if type_ == "national":
            data_list = [
                [d["to"], d["intensity"].get("forecast", nan), d["intensity"].get("actual", nan),
                 d["intensity"]["index"]] for d in response.get("data", [])
            ]
            data = pd.DataFrame(data_list, columns=["timestamp", "forecast", "actual", "index"])
            data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True, infer_datetime_format=True)
            return data, None
        else:
            carbon_list = []
            fuel_mix_list = []
            region = response.get("data", {})
            for datum in region.get("data", []):
                carbon_list.append([datum["to"], region.get("regionid"),
                                    datum["intensity"].get("forecast", nan),
                                    datum["intensity"].get("actual", nan),
                                    datum["intensity"]["index"]])
                fuel_mix = {f["fuel"]: f.get("perc", nan) for f in datum.get("generationmix", [])}
                fuel_mix_list.append([datum["to"], region.get("regionid")] + [fuel_mix.get(l, nan) for l in self.FUEL_MIX_LABELS])

            carbon_data = pd.DataFrame(carbon_list, columns=["timestamp", "regionid", "forecast", "actual", "index"])
            carbon_data["timestamp"] = pd.to_datetime(carbon_data["timestamp"], utc=True, infer_datetime_format=True)

            fuel_mix_data = pd.DataFrame(fuel_mix_list, columns=["timestamp", "regionid"] + self.FUEL_MIX_LABELS)
            fuel_mix_data["timestamp"] = pd.to_datetime(fuel_mix_data["timestamp"], utc=True, infer_datetime_format=True)

            return carbon_data, fuel_mix_data
