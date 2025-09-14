import argparse
import logging
from src.chargeplace.chargeplace_scotland_api import ChargePlaceScotlandAPI


def build_parser():
    p = argparse.ArgumentParser(description='GridCharge Dataset Pipelline.')
    p.add_argument('--feature-collection', default='data/source/feature_collection.json')
    p.add_argument('--sessions', default='data/source/all_sessions.csv')
    p.add_argument('--council-shp', default='data/source/geo_data/pub_commcnc.shp')
    p.add_argument('--council-csv', default='data/source/council_areas.csv')
    p.add_argument('--base-dir', default='data/result')
    return p


def main():
    logging.basicConfig(level=logging.INFO)
    args = build_parser().parse_args()

    api = ChargePlaceScotlandAPI(args.feature_collection,
                                 args.sessions,
                                 args.council_shp,
                                 args.council_csv)

    api.create_folder_structure(base_dir=args.base_dir)
    api.locate_council_area_charging_infrastructure(base_dir=args.base_dir)
    # Prefetch weather data for each CP ID over its active session date range
    # api.fetch_and_store_weather()
    api.populate_session_data_per_charger(base_dir=args.base_dir)


if __name__ == '__main__':
    main()
