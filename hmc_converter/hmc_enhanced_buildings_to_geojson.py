import json
import os
import re

import geojson
from progressbar import ProgressBar

import hmc_layer_cross_referencing

input_layers = ['enhanced-buildings']

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
            for layer in input_layers:
                if re.match('^{}_.*\.json$'.format(layer), f):
                    hmc_decoded_json_file_path = os.path.join(r, f)
                    print(hmc_decoded_json_file_path)
                    with open(hmc_decoded_json_file_path, mode='r', encoding='utf-8') as hmc_json:
                        output_geojson_file_path = os.path.join(r, '{}_location.geojson'.format(f))
                        if os.path.exists(output_geojson_file_path) and overwrite_result != 'y':
                            print('{} --> existing already.'.format(output_geojson_file_path))
                        else:
                            building_footprints_reference_list = hmc_layer_cross_referencing.geojson_file_reader(
                                r, 'building-footprints')

                            with open(output_geojson_file_path, mode='w', encoding='utf-8') as output_geojson:
                                hmc_json = json.loads(hmc_json.read())
                                partition_name = hmc_json.get('partitionName')
                                building_list = hmc_json.get('buildings')
                                place_ref_list = hmc_json.get('placeRefs')
                                address_ref_list = hmc_json.get('addressRefs')
                                tenant_space_list = hmc_json.get('tenantSpaces')
                                address_point_associated_with_building_list = hmc_json.get(
                                    'addressPointsAssociatedWithBuildings')
                                place_associated_with_building_list = hmc_json.get('placesAssociatedWithBuildings')
                                tenant_spaces_within_building_list = hmc_json.get('tenantSpacesWithinBuildings')

                                building_feature_list = []

                                buildings_process_progressbar = ProgressBar(min_value=0, max_value=len(building_list),
                                                                            prefix='{} - mapping buildings:'.format(f))
                                building_index = 0
                                for building in building_list:
                                    buildings_process_progressbar.update(building_index)
                                    building_index += 1
                                    building_footprint_loc_ref = building['buildingFootprintLocRef']
                                    building_footprint_loc_ref_identifier = building_footprint_loc_ref['identifier']
                                    for building_footprints_reference_feature_collection in building_footprints_reference_list.features:
                                        for building_footprints_reference_feature in building_footprints_reference_feature_collection.features:
                                            if building_footprints_reference_feature.properties.get(
                                                    'polygonType') == 'boundary':
                                                if building_footprints_reference_feature.properties['location'][
                                                    'identifier'] == building_footprint_loc_ref_identifier:
                                                    building_feature_list.append(building_footprints_reference_feature)
                                buildings_process_progressbar.finish()
                                buildings_process_progressbar = ProgressBar(min_value=0, max_value=len(
                                    address_point_associated_with_building_list),
                                                                            prefix='{} - mapping addresses:'.format(f))
                                building_index = 0
                                for address_point_associated_with_building in address_point_associated_with_building_list:
                                    building_index += 1
                                    building_ref_index = address_point_associated_with_building.get('buildingRefIndex')
                                    address_point_ref_index = address_point_associated_with_building.get(
                                        'addressPointRefIndex')
                                    confidence_score = address_point_associated_with_building.get('confidenceScore')
                                    if building_ref_index:
                                        if not building_feature_list[building_ref_index].properties.get(
                                                'address_point_ref'):
                                            building_feature_list[building_ref_index].properties['address_point_ref'] = []
                                        if address_point_ref_index:
                                            address_ref_id = address_ref_list[address_point_ref_index]['identifier']
                                            building_feature_list[building_ref_index].properties['address_point_ref'].append(
                                                {'addressRefId': address_ref_id, 'confidenceScore': confidence_score})
                                buildings_process_progressbar.finish()
                                buildings_process_progressbar = ProgressBar(min_value=0, max_value=len(
                                    place_associated_with_building_list),
                                                                            prefix='{} - mapping places:'.format(f))
                                building_index = 0
                                for place_associated_with_building in place_associated_with_building_list:
                                    building_index += 1
                                    building_ref_index = place_associated_with_building.get('buildingRefIndex')
                                    place_ref_index = place_associated_with_building.get('placeRefIndex')
                                    confidence_score = address_point_associated_with_building.get('confidenceScore')
                                    if building_ref_index:
                                        if not building_feature_list[building_ref_index].properties.get('place_ref'):
                                            building_feature_list[building_ref_index].properties['place_ref'] = []
                                        place_ref_id = place_ref_list[place_ref_index]['identifier']
                                        building_feature_list[building_ref_index].properties['place_ref'].append(
                                            {'placeRefId': place_ref_id, 'confidenceScore': confidence_score})
                                if tenant_spaces_within_building_list:
                                    tenant_space_index = 0
                                    for tenant_spaces_within_building in tenant_spaces_within_building_list:
                                        building_ref_index = tenant_spaces_within_building.get('buildingRefIndex')
                                        building_feature_list[building_ref_index].properties['tenantSpaces'] = []
                                        building_feature_list[building_ref_index].properties['tenantSpaces'].append(
                                            {'confidenceScore': tenant_spaces_within_building['confidenceScore'],
                                             'tenantSpaces': tenant_space_list[tenant_space_index]})

                                buildings_process_progressbar.finish()
                                output_geojson.write(
                                    json.dumps(geojson.FeatureCollection(building_feature_list), indent='    '))
