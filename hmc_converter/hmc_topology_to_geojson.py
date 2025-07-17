import json
import os
import re

import geojson
from here.content.utils.hmc_external_references import HMCExternalReferences
from here.content.utils.hmc_external_references import Ref
from here.platform.adapter import Identifier
from here.platform.adapter import Partition
from progressbar import ProgressBar


class HmcTopologyToGeoJson:
    def convert(self, partition_folder_path):
        for r, d, fs in os.walk(partition_folder_path):
            for f in fs:
                if re.match('^topology-geometry_.*\.json$', f):
                    hmc_decoded_json_file_path = os.path.join(r, f)
                    print(hmc_decoded_json_file_path)
                    output_geojson_file_path = os.path.join(r, '{}.geojson'.format(f))
                    if not 'overwrite_result' in globals():
                        overwrite_result = 'n'
                    else:
                        overwrite_result = 'y'
                    if os.path.exists(output_geojson_file_path) and overwrite_result != 'y':
                        print('{} --> existing already.'.format(output_geojson_file_path))
                        return output_geojson_file_path
                    else:
                        node_feature_list = []
                        segment_feature_list = []

                        # hmc_external_reference = HMCExternalReferences()

                        with open(hmc_decoded_json_file_path, mode='r', encoding='utf-8') as hmc_json:
                            with open(output_geojson_file_path, mode='w', encoding='utf-8') as output_geojson:
                                hmc_json = json.loads(hmc_json.read())
                                partion_name = hmc_json['partitionName']
                                node_list = hmc_json['node']
                                segment_list = hmc_json['segment']
                                node_process_progressbar = ProgressBar(min_value=0, max_value=len(
                                    node_list), prefix='{} - processing nodes:'.format(
                                    os.path.basename(hmc_decoded_json_file_path)))
                                node_index = 0
                                for node in node_list:
                                    node_process_progressbar.update(node_index)
                                    node_index += 1
                                    node_feature = geojson.Feature()
                                    node_geometry = geojson.geometry.Geometry()
                                    node_geometry.type = 'Point'
                                    node_geometry.coordinates = [node['geometry']['longitude'],
                                                                 node['geometry']['latitude']]
                                    node_feature.geometry = node_geometry
                                    for node_key in list(node.keys()):
                                        node_feature.properties[node_key] = node[node_key]
                                    node_feature_list.append(node_feature)
                                node_feature_collection = geojson.FeatureCollection(node_feature_list)
                                node_feature_collection['properties'] = [{'featureType': 'node'}]
                                node_process_progressbar.finish()

                                segment_process_progressbar = ProgressBar(min_value=0, max_value=len(
                                    segment_list), prefix='{} - processing segments:'.format(
                                    os.path.basename(hmc_decoded_json_file_path)))
                                segment_index = 0
                                for segment in segment_list:
                                    segment_process_progressbar.update(segment_index)
                                    segment_keys = list(segment.keys())
                                    segment_feature = geojson.Feature()
                                    segment_geometry = geojson.geometry.Geometry()
                                    segment_geometry.type = 'LineString'
                                    coordinates = []
                                    for latlng_pair in segment['geometry']['point']:
                                        lat = latlng_pair['latitude']
                                        lng = latlng_pair['longitude']
                                        coordinates.append([lng, lat])
                                    segment_geometry.coordinates = coordinates
                                    segment_feature.geometry = segment_geometry
                                    for key in segment_keys:
                                        segment_feature.properties[key] = segment[key]
                                    # hmcExternalReference - start
                                    # segment_feature.properties['hmcExternalReference'] = {}
                                    # segment_feature.properties['hmcExternalReference'][
                                    #     'pvid'] = hmc_external_reference.segment_to_pvid(
                                    #     partition_id=partion_name,
                                    #     segment_ref=Ref(partition=Partition(str(partion_name)),
                                    #                     identifier=Identifier(segment['identifier'])))
                                    # hmcExternalReference - end
                                    segment_feature_list.append(segment_feature)
                                    segment_index += 1
                                segment_process_progressbar.finish()
                                segment_feature_collection = geojson.FeatureCollection(segment_feature_list)
                                segment_feature_collection['properties'] = [{'featureType': 'segment'}]
                                topology_feature_collection = geojson.FeatureCollection(
                                    [node_feature_collection, segment_feature_collection])
                                output_geojson.write(json.dumps(topology_feature_collection, indent='    '))
                        return output_geojson_file_path


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('partition_path', help='path of partition folder', type=str)
    parser.add_argument('overwrite_result', help='overwrite geojson result file (y/N)', nargs='?', default='n',
                        type=str)
    args = parser.parse_args()
    partition_folder_path = args.partition_path
    overwrite_result = str.lower(args.overwrite_result)

    HmcTopologyToGeoJson().convert(partition_folder_path)
