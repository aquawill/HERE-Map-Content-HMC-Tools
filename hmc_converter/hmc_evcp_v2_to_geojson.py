import json
import os
import re

import geojson
from progressbar import ProgressBar

input_layers = ['electric-vehicle-charging-locations']

feature_list = []

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('partition_path', help='path of partition folder', type=str)
    parser.add_argument('overwrite_result', help='overwrite geojson result file (y/N)', nargs='?', default='n',
                        type=str)
    args = parser.parse_args()
    partition_folder_path = args.partition_path
    overwrite_result = str.lower(args.overwrite_result)

    for r, d, fs in os.walk(partition_folder_path):
        for f in fs:
            for point_feature_layer in input_layers:
                if re.match('^{}_.*\.json$'.format(point_feature_layer), f):
                    hmc_decoded_json_file_path = os.path.join(r, f)
                    print(hmc_decoded_json_file_path)
                    output_geojson_file_path = os.path.join(r, '{}.geojson'.format(f))
                    if os.path.exists(output_geojson_file_path) and overwrite_result != 'y':
                        print('{} --> existing already.'.format(output_geojson_file_path))
                    else:
                        feature_list = []
                        with open(hmc_decoded_json_file_path, mode='r', encoding='utf-8') as hmc_json:
                            with open(output_geojson_file_path, mode='w', encoding='utf-8') as output_geojson:
                                hmc_json = json.loads(hmc_json.read())
                                del hmc_json['partitionName']

                                charging_location_list = hmc_json['chargingLocation']

                                charging_location_process_progressbar = ProgressBar(min_value=0, max_value=len(
                                    charging_location_list), prefix='{} - processing evse:'.format(
                                    os.path.basename(hmc_decoded_json_file_path)))
                                charging_location_index = 0

                                for charging_location in charging_location_list:
                                    charging_location_process_progressbar.update(charging_location_index)
                                    charging_location_index += 1
                                    point = geojson.Point()
                                    feature = geojson.Feature()
                                    geometry = geojson.geometry.Geometry()
                                    geometry.coordinates = [charging_location['geometry']['longitude'],
                                                            charging_location['geometry']['latitude']]
                                    geometry.type = 'Point'
                                    feature.geometry = geometry

                                    manual_key_mapping_list = {'operatorIndex': 'businessDetails',
                                                               'suboperatorIndex': 'businessDetails',
                                                               'ownerIndex': 'businessDetails',
                                                               'eMobilityServiceProvidersIndex': 'businessDetails',
                                                               'evsesIndex': 'evse'}

                                    for key in list(charging_location.keys()):
                                        if str(key).endswith('Index'):
                                            attribute_key_name = str(key)[:-5]
                                            if not hmc_json.get(attribute_key_name):
                                                key_index = charging_location['{}Index'.format(attribute_key_name)]
                                                del charging_location['{}Index'.format(attribute_key_name)]

                                                if isinstance(key_index, int):
                                                    charging_location[attribute_key_name] = \
                                                    hmc_json[manual_key_mapping_list[key]][key_index]
                                                elif isinstance(key_index, list):
                                                    key_index_list = key_index
                                                    charging_location_attribute_list = []
                                                    for index in key_index_list:
                                                        charging_location_attribute_list.append(
                                                            hmc_json[manual_key_mapping_list[key]][index])
                                                        charging_location[
                                                            attribute_key_name] = charging_location_attribute_list
                                            else:
                                                key_index = charging_location['{}Index'.format(attribute_key_name)]
                                                del charging_location['{}Index'.format(attribute_key_name)]
                                                charging_location[attribute_key_name] = hmc_json[attribute_key_name][
                                                    key_index]

                                    feature.properties = charging_location
                                    feature_list.append(feature)
                                charging_location_process_progressbar.finish()
                                feature_collection = geojson.FeatureCollection(feature_list)
                                output_geojson.write(json.dumps(feature_collection, indent='    '))
