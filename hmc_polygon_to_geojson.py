import json
import os
import re

import geojson
from progressbar import ProgressBar

import hmc_layer_cross_referencing

polygon_feature_layers = ['buildings', '3d-buildings', 'building-footprints', 'cartography', 'postal-area-boundaries']


# 'administrative-locations' is excluded

def polygonal_feature_objects_parser(polygonal_feature_object_list):
    polygonal_feature_object_output_list = []
    polygonal_feature_object_process_progressbar = ProgressBar(min_value=0, max_value=len(
        location_list), prefix='{} - processing locations:'.format(f))
    polygonal_feature_object_index = 0
    for polygonal_feature_object in polygonal_feature_object_list:
        polygonal_feature_object_process_progressbar.update(polygonal_feature_object_index)
        location_element_list = []

        if polygonal_feature_object.get('alternateGeometry'):
            del polygonal_feature_object['alternateGeometry']

        if polygonal_feature_object.get('displayPosition'):
            location_display_position_feature = geojson.Feature()
            location_display_position_feature_geometry = geojson.geometry.Geometry()
            location_display_position_feature_geometry.type = 'Point'
            location_display_position_feature_geometry.coordinates = [
                polygonal_feature_object['displayPosition']['longitude'],
                polygonal_feature_object['displayPosition']['latitude']]
            location_display_position_feature.geometry = location_display_position_feature_geometry
            location_element_list.append(location_display_position_feature)
            del polygonal_feature_object['displayPosition']

        if polygonal_feature_object.get('boundingBox'):
            bounding_box_feature = geojson.Feature()
            bounding_box_feature_geometry = geojson.geometry.Geometry()
            bounding_box_feature_geometry.type = 'Polygon'
            bounding_box_feature_geometry_nw = [polygonal_feature_object['boundingBox']['westLongitude'],
                                                polygonal_feature_object['boundingBox']['northLatitude']]
            bounding_box_feature_geometry_sw = [polygonal_feature_object['boundingBox']['westLongitude'],
                                                polygonal_feature_object['boundingBox']['southLatitude']]
            bounding_box_feature_geometry_se = [polygonal_feature_object['boundingBox']['eastLongitude'],
                                                polygonal_feature_object['boundingBox']['southLatitude']]
            bounding_box_feature_geometry_ne = [polygonal_feature_object['boundingBox']['eastLongitude'],
                                                polygonal_feature_object['boundingBox']['northLatitude']]
            bounding_box_feature_geometry.coordinates = [[bounding_box_feature_geometry_nw,
                                                          bounding_box_feature_geometry_sw,
                                                          bounding_box_feature_geometry_se,
                                                          bounding_box_feature_geometry_ne,
                                                          bounding_box_feature_geometry_nw]]
            bounding_box_feature.geometry = bounding_box_feature_geometry
            bounding_box_feature.properties = {'polygonType': 'boundingBox'}
            location_element_list.append(bounding_box_feature)
            del polygonal_feature_object['boundingBox']

        if polygonal_feature_object.get('geometry'):
            geometry_level_2_name = list(polygonal_feature_object['geometry'].keys())[0]
            geometry_level_3_name = list(polygonal_feature_object['geometry'][geometry_level_2_name].keys())[0]
            location_geometry_multi_component_list = \
                polygonal_feature_object['geometry'][geometry_level_2_name][
                    geometry_level_3_name]
            for location_geometry_multi_component in location_geometry_multi_component_list:

                polygon_exteriorRing_feature = geojson.Feature()
                polygon_exteriorRing_feature_geometry = geojson.geometry.Geometry()
                polygon_exteriorRing_feature_geometry.type = ''
                polygon_exteriorRing_feature_geometry_list = []
                if location_geometry_multi_component.get('polygon'):
                    # 3d-buildings
                    polygon_exteriorRing_point_list = \
                        location_geometry_multi_component['polygon']['exteriorRing']['point']
                    polygon_exteriorRing_feature_geometry.type = 'Polygon'
                elif location_geometry_multi_component.get('exteriorRing'):
                    # building-footprints
                    polygon_exteriorRing_point_list = \
                        location_geometry_multi_component['exteriorRing']['point']
                    polygon_exteriorRing_feature_geometry.type = 'Polygon'
                else:
                    polygon_exteriorRing_point_list = location_geometry_multi_component
                    polygon_exteriorRing_feature_geometry.type = 'LineString'
                if location_geometry_multi_component.get('heightClearance'):
                    polygonal_feature_object['heightClearance'] = location_geometry_multi_component.get(
                        'heightClearance')
                if not isinstance(polygon_exteriorRing_point_list, list):
                    polygon_exteriorRing_point_list = polygon_exteriorRing_point_list.get(
                        'point')
                for polygon_exteriorRing_point in polygon_exteriorRing_point_list:
                    polygon_exteriorRing_point_lat = polygon_exteriorRing_point['latitude']
                    polygon_exteriorRing_point_lng = polygon_exteriorRing_point['longitude']
                    polygon_exteriorRing_feature_geometry_list.append(
                        [polygon_exteriorRing_point_lng, polygon_exteriorRing_point_lat])
                polygon_exteriorRing_feature_geometry.coordinates = [
                    polygon_exteriorRing_feature_geometry_list]
                polygon_exteriorRing_feature.geometry = polygon_exteriorRing_feature_geometry
                polygon_exteriorRing_feature.properties = {'polygonType': 'boundary'}
                location_element_list.append(polygon_exteriorRing_feature)

            del polygonal_feature_object['geometry']

        for location_element in location_element_list:
            location_element.properties['location'] = polygonal_feature_object
            if 'named_place_list' in locals():
                if named_place_list:
                    for named_place in named_place_list:
                        # print(location.get('identifier'), named_place['identifier'])
                        if named_place['locationRef']['identifier'] == polygonal_feature_object.get('identifier'):
                            polygonal_feature_object['namedPlace'] = named_place

        if hmc_json.get('place'):
            place_list = hmc_json['place']
            for place in place_list:
                place_building_location_identifier = place['locationRef']['identifier']
                for location_element in location_element_list:
                    if location_element.properties['location'][
                        'identifier'] == place_building_location_identifier:
                        location_element.properties['place'] = place
        polygonal_feature_object_index += 1
        polygonal_feature_object_output_list.append(geojson.FeatureCollection(location_element_list))
    polygonal_feature_object_process_progressbar.finish()
    return polygonal_feature_object_output_list


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
                        output_geojson_file_path = os.path.join(r, '{}.geojson'.format(f))
                        if os.path.exists(output_geojson_file_path) and overwrite_result != 'y':
                            print('{} --> existing already.'.format(output_geojson_file_path))
                        else:
                            with open(output_geojson_file_path, mode='w', encoding='utf-8') as output_geojson:
                                hmc_json = json.loads(hmc_json.read())
                                partition_name = hmc_json['partitionName']
                                named_place_list: list
                                if polygon_feature_layer == 'administrative-locations':
                                    named_place_list = hmc_layer_cross_referencing.named_place_list_generator(r)
                                location_list = hmc_json['location']
                                location_output_list = polygonal_feature_objects_parser(location_list)
                                output_geojson.write(
                                    json.dumps((geojson.FeatureCollection(location_output_list)), indent='    '))
