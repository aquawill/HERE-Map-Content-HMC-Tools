import json
import os
import re

import geojson

polygon_feature_layers = ['postal-code-points']

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
            for polygon_feature_layer in polygon_feature_layers:
                if re.match('^{}_.*\.json$'.format(polygon_feature_layer), f):
                    hmc_decoded_json_file_path = os.path.join(r, f)
                    print(hmc_decoded_json_file_path)
                    with open(hmc_decoded_json_file_path, mode='r', encoding='utf-8') as hmc_json:
                        output_geojson_file_path = os.path.join(r,
                                                                '{}.geojson'.format(f))
                        if os.path.exists(output_geojson_file_path) and overwrite_result != 'y':
                            print('{} --> existing already.'.format(output_geojson_file_path))
                        else:
                            with open(output_geojson_file_path, mode='w', encoding='utf-8') as output_geojson:
                                hmc_json = json.loads(hmc_json.read())
                                partition_name = hmc_json['partitionName']
                                postal_code_point_list = hmc_json['postalCodePoints']
                                feature_list = []
                                for postal_code_point in postal_code_point_list:
                                    postal_code_point_feature = geojson.Feature()
                                    feature = geojson.Feature()
                                    geometry = geojson.geometry.Geometry()
                                    geometry.type = 'Point'
                                    geometry.coordinates = [postal_code_point['postalCodeCentroid']['longitude'],
                                                            postal_code_point['postalCodeCentroid']['latitude']]
                                    feature.geometry = geometry
                                    del postal_code_point['postalCodeCentroid']
                                    feature.properties = postal_code_point
                                    feature_list.append(feature)
                                output_geojson.write(json.dumps(geojson.FeatureCollection(feature_list), indent='    '))
