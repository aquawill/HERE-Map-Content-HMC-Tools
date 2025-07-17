import json
import os
import re

import geojson

input_layers = ['landmarks-3d', 'landmarks-2d']

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
                                landmark_list = hmc_json['landmark']
                                landmark_feature_list = []
                                for landmark in landmark_list:
                                    landmark_feature = geojson.Feature()
                                    landmark_feature.geometry = geojson.geometry.Geometry()
                                    landmark_feature.geometry.type = 'Point'
                                    landmark_feature_geometry_geometry_coordinates = [landmark['anchorPoint']['longitude'],
                                                                                      landmark['anchorPoint']['latitude']]
                                    landmark_feature.geometry.coordinates = landmark_feature_geometry_geometry_coordinates
                                    del landmark['anchorPoint']
                                    for key in list(landmark.keys()):
                                        landmark_feature.properties[key] = landmark[key]
                                    landmark_feature_list.append(landmark_feature)
                                landmark_feature_collection = geojson.FeatureCollection(landmark_feature_list)
                                output_geojson.write(str(landmark_feature_collection))
