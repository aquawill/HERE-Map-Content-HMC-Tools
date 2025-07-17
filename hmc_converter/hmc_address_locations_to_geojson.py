import json
import os
import re

import geojson
from here.platform import Platform
from progressbar import ProgressBar

import hmc_layer_cross_referencing
from hmc_download_options import FileFormat
from hmc_downloader import HmcDownloader

polygon_feature_layers = ['address-locations']

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
                    partition_version = int(re.search(r'v(\d+)', f).group(1))
                    hmc_decoded_json_file_path = os.path.join(r, f)
                    print(hmc_decoded_json_file_path)
                    address_attributes_reference_list = hmc_layer_cross_referencing.geojson_file_reader(
                        r, 'address-attributes')
                    with open(hmc_decoded_json_file_path, mode='r', encoding='utf-8') as hmc_json:
                        location_output_geojson_file_path = os.path.join(r, '{}_location.geojson'.format(f))
                        if os.path.exists(location_output_geojson_file_path) and overwrite_result != 'y':
                            print('{} --> existing already.'.format(location_output_geojson_file_path))
                        else:
                            with open(location_output_geojson_file_path, mode='w',
                                      encoding='utf-8') as location_output_geojson:
                                hmc_json = json.loads(hmc_json.read())
                                partition_name = hmc_json['partitionName']
                                location_list = hmc_json['location']
                                location_output_list = []
                                location_process_progressbar = ProgressBar(min_value=0, max_value=len(
                                    location_list), prefix='{} - processing locations:'.format(f))
                                location_index = 0
                                for location in location_list:
                                    location_process_progressbar.update(location_index)
                                    location_index += 1
                                    location_element_list = []
                                    if location.get('displayPosition'):
                                        location_display_position_feature = geojson.Feature()
                                        location_display_position_feature_geometry = geojson.geometry.Geometry()
                                        location_display_position_feature_geometry.type = 'Point'
                                        location_display_position_feature_geometry.coordinates = [
                                            location['displayPosition']['longitude'],
                                            location['displayPosition']['latitude']]
                                        del location['displayPosition']
                                        location_display_position_feature.geometry = location_display_position_feature_geometry
                                        location_display_position_feature.properties = location
                                        location_output_list.append(location_display_position_feature)
                                location_feature_collection = geojson.FeatureCollection(location_output_list)
                                location_process_progressbar.finish()
                                location_output_geojson.write(json.dumps(location_feature_collection, indent='    '))
                            address_output_geojson_file_path = os.path.join(r, '{}_address.geojson'.format(f))
                            with open(address_output_geojson_file_path, mode='w',
                                      encoding='utf-8') as address_output_geojson:
                                postal_code_list = hmc_json.get('postalCode')
                                address_list = hmc_json['address']
                                address_process_progressbar = ProgressBar(min_value=0, max_value=len(
                                    address_list), prefix='{} - processing addresses:'.format(f))
                                address_process_index = 0
                                address_feature_list = []
                                from_street_section_ref_partition_name_set = set()
                                for address in address_list:
                                    address_process_progressbar.update(address_process_index)
                                    address_process_index += 1
                                    if address.get('fromStreetSectionRef'):
                                        from_street_section_ref = address['fromStreetSectionRef']
                                        from_street_section_ref_partition_name = from_street_section_ref[
                                            'partitionName']
                                        from_street_section_ref_identifier = from_street_section_ref['identifier']
                                        from_street_section_ref_partition_name_set.add(
                                            from_street_section_ref_partition_name)
                                        for address_attribute_reference in address_attributes_reference_list.features:
                                            if address_attribute_reference.properties['properties']['streetSection'][
                                                'streetSectionRef']['identifier'] == from_street_section_ref_identifier:
                                                address_feature = geojson.Feature()
                                                address_feature_geometry = address_attribute_reference.geometry
                                                address_feature.geometry = address_feature_geometry
                                                address_feature.properties = address
                                                # address_feature.properties['house'] = address['house']
                                                if address.get('postalMapping'):
                                                    address_postal_mapping_list = address['postalMapping']
                                                    address_feature_postal_code_list = []
                                                    for address_feature_postal_mapping in address_postal_mapping_list:
                                                        if address_feature_postal_mapping.get('postalCodeIndex'):
                                                            address_feature_postal_mapping_index = address_feature_postal_mapping.get(
                                                                'postalCodeIndex')
                                                            address_feature_postal_code_list.append(
                                                                postal_code_list[address_feature_postal_mapping_index])
                                                            address_feature_postal_mapping[
                                                                'postalCode'] = address_feature_postal_code_list
                                                    address_feature.properties[
                                                        'postalMapping'] = address_feature_postal_code_list
                                                address_feature_list.append(address_feature)
                                address_feature_collection = geojson.FeatureCollection(address_feature_list)
                                address_process_progressbar.finish()
                                for admin_partition in list(from_street_section_ref_partition_name_set):
                                    from_street_section_ref_partition_name_set.add(admin_partition.split('-')[0])
                                print('admin reference partitions: ', list(from_street_section_ref_partition_name_set))
                                admin_reference_layer_list = ['administrative-places', 'administrative-locations',
                                                              'administrative-place-profiles']
                                print(
                                    'download admin reference layers: {}'.format(', '.join(admin_reference_layer_list)))
                                platform = Platform()
                                env = platform.environment
                                config = platform.platform_config
                                print('HERE Platform Status: ', platform.get_status())
                                platform_catalog = platform.get_catalog(hrn='hrn:here:data::olp-here:rib-2')
                                for admin_partition in list(from_street_section_ref_partition_name_set):
                                    for admin_reference_layer in admin_reference_layer_list:
                                        HmcDownloader(catalog=platform_catalog, layer=admin_reference_layer,
                                                      file_format=FileFormat.JSON).download_generic_layer(
                                            quad_ids=[admin_partition], version=partition_version)
                                address_output_geojson.write(json.dumps(address_feature_collection, indent='    '))
