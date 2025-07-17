import json
import os
import re

import geojson
from progressbar import ProgressBar

input_layers = ['distance-markers', 'electric-vehicle-charging-stations', 'here-fueling-stations',
                'here-places-essential-map', 'here-places', 'here-truck-service-locations', 'warning-locations']

feature_list = []


def place_index_attribute_mapping(attribute_name):
    attribute_list = hmc_json[attribute_name]
    attribute_progressbar = ProgressBar(min_value=0, max_value=len(
        attribute_list), prefix='{} - processing {}:'.format(f, attribute_name))
    attribute_index = 0
    for attribute in attribute_list:
        attribute_progressbar.update(attribute_index)
        attribute_index += 1
        attribute_place_index = attribute['placeIndex']
        del attribute['placeIndex']
        for attribute_place_index in attribute_place_index:
            feature_list[attribute_place_index].properties[attribute_name] = attribute
    del hmc_json[attribute_name]
    attribute_progressbar.finish()


def attribute_list_mapping(attribute_name):
    attribute_list = hmc_json[attribute_name]
    attribute_list_size = len(attribute_list)
    attribute_list = hmc_json[attribute_name]
    attribute_progressbar = ProgressBar(min_value=0, max_value=len(
        attribute_list), prefix='{} - processing {}:'.format(f, attribute_name))
    for i in range(attribute_list_size):
        attribute_progressbar.update(i)
        feature_list[i].properties[attribute_name] = attribute_list[i]
    del hmc_json[attribute_name]
    attribute_progressbar.finish()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('partition_path', help='path of partition folder', type=str)
    parser.add_argument('overwrite_result', help='overwrite geojson result file (y/N)', nargs='?', default='n', type=str)
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

                                place_list = hmc_json['place']

                                # parse locations to geojson points
                                location_list = hmc_json['location']

                                location_process_progressbar = ProgressBar(min_value=0, max_value=len(
                                    place_list), prefix='{} - processing locations:'.format(
                                    os.path.basename(hmc_decoded_json_file_path)))
                                location_index = 0
                                # create basic geojson feature collection
                                for location in location_list:
                                    location_process_progressbar.update(location_index)
                                    location_index += 1

                                    point = geojson.Point()
                                    feature = geojson.Feature()
                                    geometry = geojson.geometry.Geometry()
                                    geometry.type = str(location['locationType']).capitalize()
                                    if location.get('displayPosition') is not None:
                                        geometry.coordinates = [location['displayPosition']['longitude'],
                                                                location['displayPosition']['latitude']]
                                    elif location.get('geometry').get('point') is not None:
                                        geometry.coordinates = [location['geometry']['point']['longitude'],
                                                                location['geometry']['point']['latitude']]
                                    feature.geometry = geometry
                                    feature.properties['location'] = location
                                    if location.get('addressIndex') is not None:
                                        address_list = hmc_json['address']
                                        feature.properties['address'] = address_list[location.get('addressIndex')]
                                    feature_list.append(feature)
                                del hmc_json['location']
                                if location.get('addressIndex') is not None:
                                    del hmc_json['address']
                                location_process_progressbar.finish()

                                hmc_json_keys = list(hmc_json.keys())

                                for key in hmc_json_keys:
                                    if len(hmc_json[key]) > 0:
                                        if hmc_json[key][0].get('placeIndex') is not None:
                                            place_index_attribute_mapping(key)
                                    else:
                                        attribute_list_mapping(key)

                                feature_collection = geojson.FeatureCollection(feature_list)
                                output_geojson.write(json.dumps(feature_collection, indent='    '))
