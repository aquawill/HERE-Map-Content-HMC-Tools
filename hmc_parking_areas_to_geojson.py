import json
import os
import re

import geojson
from progressbar import ProgressBar

input_layers = ['parking-areas']

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
                        output_geojson_file_path = os.path.join(r,
                                                                '{}_location.geojson'.format(f))
                        if os.path.exists(output_geojson_file_path) and overwrite_result != 'y':
                            print('{} --> existing already.'.format(output_geojson_file_path))
                        else:
                            with open(output_geojson_file_path, mode='w', encoding='utf-8') as output_geojson:
                                hmc_json = json.loads(hmc_json.read())
                                partition_name = hmc_json['partitionName']
                                parking_area_list = hmc_json['parkingAreas']
                                place_ref_list = hmc_json['placeRefs']
                                address_ref_list = hmc_json['addressRefs']
                                places_served_by_parking_area_list = hmc_json['placesServedByParkingAreas']
                                addresses_associated_with_parking_area_list = hmc_json[
                                    'addressesAssociatedWithParkingAreas']
                                parking_area_feature_list = []
                                parking_area_process_progressbar = ProgressBar(min_value=0,
                                                                               max_value=len(parking_area_list),
                                                                               prefix='{} - processing buildings:'.format(
                                                                                   f))
                                parking_area_index = 0
                                for parking_area in parking_area_list:
                                    parking_area_process_progressbar.update(parking_area_index)
                                    parking_area_index += 1
                                    if parking_area.get('geometry'):
                                        geometry_level_2_name = list(parking_area['geometry'].keys())[0]
                                        geometry_level_3_name = \
                                            list(parking_area['geometry'][geometry_level_2_name].keys())[0]
                                        location_geometry_multi_component_list = \
                                            parking_area['geometry'][geometry_level_2_name][
                                                geometry_level_3_name]
                                        for location_geometry_multi_component in location_geometry_multi_component_list:
                                            polygon_exteriorRing_feature = geojson.Feature()
                                            polygon_exteriorRing_feature_geometry = geojson.geometry.Geometry()
                                            polygon_exteriorRing_feature_geometry.type = ''
                                            polygon_exteriorRing_feature_geometry_list = []
                                            if location_geometry_multi_component.get('polygon'):
                                                polygon_exteriorRing_point_list = \
                                                    location_geometry_multi_component['polygon']['exteriorRing']['point']
                                                polygon_exteriorRing_feature_geometry.type = 'Polygon'
                                            elif location_geometry_multi_component.get('exteriorRing'):
                                                polygon_exteriorRing_point_list = \
                                                    location_geometry_multi_component['exteriorRing']['point']
                                                polygon_exteriorRing_feature_geometry.type = 'Polygon'
                                            else:
                                                polygon_exteriorRing_point_list = location_geometry_multi_component
                                                polygon_exteriorRing_feature_geometry.type = 'LineString'
                                            for polygon_exteriorRing_point in polygon_exteriorRing_point_list:
                                                polygon_exteriorRing_point_lat = polygon_exteriorRing_point['latitude']
                                                polygon_exteriorRing_point_lng = polygon_exteriorRing_point['longitude']
                                                polygon_exteriorRing_feature_geometry_list.append(
                                                    [polygon_exteriorRing_point_lng, polygon_exteriorRing_point_lat])
                                            polygon_exteriorRing_feature_geometry.coordinates = [
                                                polygon_exteriorRing_feature_geometry_list]
                                            polygon_exteriorRing_feature.geometry = polygon_exteriorRing_feature_geometry
                                            polygon_exteriorRing_feature.properties = {'polygonType': 'boundary'}
                                            parking_area_feature_list.append(polygon_exteriorRing_feature)
                                for addresses_associated_with_parking_area in addresses_associated_with_parking_area_list:
                                    parking_area_ref_index = addresses_associated_with_parking_area.get(
                                        'parkingAreaRefIndex')
                                    address_ref_index = addresses_associated_with_parking_area.get('addressRefIndex')
                                    confidence_score = addresses_associated_with_parking_area.get('confidenceScore')
                                    if parking_area_ref_index:
                                        if address_ref_index:
                                            if not parking_area_feature_list[parking_area_ref_index].properties.get(
                                                    'address_point_ref'):
                                                parking_area_feature_list[parking_area_ref_index].properties[
                                                    'addressesAssociatedWithParkingAreas'] = []
                                            address_ref_id = address_ref_list[address_ref_index]['identifier']
                                            parking_area_feature_list[parking_area_ref_index].properties[
                                                'addressesAssociatedWithParkingAreas'].append(
                                                {'addresRef': address_ref_id, 'confidenceScore': confidence_score})
                                for places_served_by_parking_area_list in places_served_by_parking_area_list:
                                    parking_area_ref_index = places_served_by_parking_area_list.get('parkingAreaRefIndex')
                                    place_ref_index = places_served_by_parking_area_list.get('placeRefIndex')
                                    confidence_score = addresses_associated_with_parking_area.get('confidenceScore')
                                    if parking_area_ref_index:
                                        if not parking_area_feature_list[parking_area_ref_index].properties.get(
                                                'placesServedByParkingAreas'):
                                            parking_area_feature_list[parking_area_ref_index].properties[
                                                'placesServedByParkingAreas'] = []
                                        place_ref_id = place_ref_list[place_ref_index]['identifier']
                                        parking_area_feature_list[parking_area_ref_index].properties[
                                            'placesServedByParkingAreas'].append(
                                            {'placeRefId': place_ref_id, 'confidenceScore': confidence_score})
                                output_geojson.write(str(geojson.FeatureCollection(parking_area_feature_list)))
                                parking_area_process_progressbar.finish()
