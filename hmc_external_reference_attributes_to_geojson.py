import json
import os
import re

import geojson
import shapely
from shapely.ops import substring
from here.platform import Platform
from progressbar import ProgressBar

import hmc_layer_cross_referencing
from hmc_download_options import FileFormat
from hmc_downloader import HmcDownloader
from hmc_topology_to_geojson import HmcTopologyToGeoJson


def topology_anchor_attribute_mapping(attribute_name, index_name):
    attribute_list = hmc_json[attribute_name]
    attribute_progressbar = ProgressBar(min_value=0, max_value=len(
        attribute_list), prefix='{} - processing {}:'.format(f, attribute_name))
    attribute_index = 0
    for attribute in attribute_list:
        attribute_progressbar.update(attribute_index)
        attribute_index += 1
        if attribute.get(index_name):
            if segment_anchor_with_attributes_list:
                attribute_segment_anchor_indexes = attribute.get(index_name)
                del attribute[index_name]
                if isinstance(attribute_segment_anchor_indexes, list):
                    for attribute_segment_anchor_index in attribute_segment_anchor_indexes:
                        segment_anchor_with_attributes_list[attribute_segment_anchor_index]['properties'][
                            attribute_name] = attribute
                elif isinstance(attribute_segment_anchor_indexes, int):
                    attribute_segment_anchor_index = attribute_segment_anchor_indexes
                    segment_anchor_with_attributes_list[attribute_segment_anchor_index]['properties'][
                        attribute_name] = attribute

        if attribute.get('nodeAnchorIndex'):
            if node_anchor_with_attributes_list:
                attribute_node_anchor_indexes = attribute.get('nodeAnchorIndex')
                del attribute['nodeAnchorIndex']
                for attribute_node_anchor_index in attribute_node_anchor_indexes:
                    if not node_anchor_with_attributes_list[attribute_node_anchor_index].get('properties'):
                        node_anchor_with_attributes_list[attribute_node_anchor_index]['properties'] = {}
                    node_anchor_with_attributes_list[attribute_node_anchor_index]['properties'][
                        attribute_name] = attribute
    attribute_progressbar.finish()


segment_anchor_with_attributes_list = []
node_anchor_with_attributes_list = []

input_layers = ['external-reference-attributes']

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
            for road_attribute_layer in input_layers:
                if re.match('^{}_.*\.json$'.format(road_attribute_layer), f):
                    hmc_decoded_json_file_path = os.path.join(r, f)
                    print(hmc_decoded_json_file_path)
                    with open(hmc_decoded_json_file_path, mode='r', encoding='utf-8') as hmc_json:
                        hmc_json = json.loads(hmc_json.read())
                        final_feature_collection = []
                        partition_name = hmc_json['partitionName']

                        segment_anchor_with_attributes_list = hmc_json.get('segmentAnchor')
                        node_anchor_with_attributes_list = hmc_json.get('nodeAnchor')

                        topology_geometry_reference_segment_list = hmc_layer_cross_referencing.segment_list_generator(r)
                        topology_geometry_reference_node_list = hmc_layer_cross_referencing.node_list_generator(r)

                        if not topology_geometry_reference_segment_list:
                            platform = Platform()
                            env = platform.environment
                            config = platform.platform_config
                            print('Download topology-geometry layer from HERE Platform...')
                            print('HERE Platform Status: ', platform.get_status())
                            platform_catalog = platform.get_catalog(hrn='hrn:here:data::olp-here:rib-2')
                            hmc_downloader_output_file_path = HmcDownloader(catalog=platform_catalog,
                                                                            layer='topology-geometry',
                                                                            file_format=FileFormat.JSON, version=None).download_partitioned_layer(
                                quad_ids=[partition_name]).get_output_file_path()

                            hmc_topology_geojson_file_path = HmcTopologyToGeoJson().convert(
                                partition_folder_path=os.path.dirname(hmc_downloader_output_file_path))
                            hmc_topology_geojson_file_dir_name = os.path.dirname(hmc_topology_geojson_file_path)
                            topology_geometry_reference_segment_list = hmc_layer_cross_referencing.segment_list_generator(
                                hmc_topology_geojson_file_dir_name)
                            topology_geometry_reference_node_list = hmc_layer_cross_referencing.node_list_generator(
                                hmc_topology_geojson_file_dir_name)

                        output_geojson_file_path = os.path.join(r, '{}.geojson'.format(f))
                        if os.path.exists(output_geojson_file_path) and overwrite_result != 'y':
                            print('{} --> existing already.'.format(output_geojson_file_path))
                        else:
                            with open(output_geojson_file_path, mode='w',
                                      encoding='utf-8') as output_geojson_file_path:
                                segment_anchor_with_attributes_index = 0
                                segment_process_progressbar = ProgressBar(min_value=0, max_value=len(
                                    segment_anchor_with_attributes_list), prefix='{} - processing segments:'.format(
                                    f))

                                for segment_anchor in segment_anchor_with_attributes_list:
                                    segment_anchor['properties'] = {}
                                for key in list(hmc_json.keys()):
                                    if isinstance(hmc_json[key], list):
                                        for hmc_json_element in hmc_json[key]:
                                            if hmc_json_element.get('segmentAnchorIndex'):
                                                topology_anchor_attribute_mapping(key, 'segmentAnchorIndex')
                                segment_anchor_with_topology_list = []
                                for segment_anchor_with_attributes in segment_anchor_with_attributes_list:
                                    segment_process_progressbar.update(segment_anchor_with_attributes_index)
                                    segment_anchor_with_attributes_index += 1
                                    oriented_segment_refs = segment_anchor_with_attributes['orientedSegmentRef']
                                    for oriented_segment_ref in oriented_segment_refs:
                                        segment_ref = oriented_segment_ref['segmentRef']
                                        segment_ref_partition_name = segment_ref['partitionName']
                                        segment_ref_identifier = segment_ref['identifier']
                                        for feature in topology_geometry_reference_segment_list['features']:
                                            if segment_ref_identifier == feature['properties']['identifier']:
                                                segment_anchor_geojson_feature = geojson.Feature()
                                                segment_start_offset = 0.0
                                                segment_end_offset = 1.0
                                                if segment_anchor_with_attributes.get('firstSegmentStartOffset'):
                                                    segment_start_offset = segment_anchor_with_attributes.get(
                                                        'firstSegmentStartOffset')
                                                if segment_anchor_with_attributes.get('lastSegmentEndOffset'):
                                                    segment_end_offset = segment_anchor_with_attributes.get(
                                                        'lastSegmentEndOffset')
                                                feature_geometry_length = shapely.LineString(
                                                    shapely.from_geojson(str(feature.geometry))).length
                                                feature_geometry_offset_length_start = feature_geometry_length * segment_start_offset
                                                feature_geometry_offset_length_end = feature_geometry_length * segment_end_offset
                                                feature_geometry_with_offsets = shapely.ops.substring(
                                                    geom=shapely.from_geojson(str(feature.geometry)),
                                                    start_dist=feature_geometry_offset_length_start,
                                                    end_dist=feature_geometry_offset_length_end)
                                                segment_anchor_geojson_feature.properties = segment_anchor_with_attributes
                                                feature_geometry_with_offsets_geojson = geojson.loads(
                                                    shapely.to_geojson(feature_geometry_with_offsets))
                                                if segment_start_offset == segment_end_offset:
                                                    segment_anchor_geojson_feature.type = 'Point'
                                                    segment_anchor_geojson_feature.geometry = geojson.geometry.Point(
                                                        feature_geometry_with_offsets_geojson)
                                                else:
                                                    segment_anchor_geojson_feature.type = 'LineString'
                                                    segment_anchor_geojson_feature.geometry = geojson.geometry.LineString(
                                                        feature_geometry_with_offsets_geojson)

                                                segment_anchor_with_topology_list.append(
                                                    segment_anchor_geojson_feature)
                                segment_anchor_with_topology_feature_collection = geojson.FeatureCollection(
                                    segment_anchor_with_topology_list)
                                segment_process_progressbar.finish()
                                final_feature_collection.append(segment_anchor_with_topology_feature_collection)
                                # output_geojson_file_path.write(
                                #     json.dumps(segment_anchor_with_topology_feature_collection, indent='    '))

                                node_anchor_with_attributes_index = 0
                                node_process_progressbar = ProgressBar(min_value=0,
                                                                       max_value=len(
                                                                           node_anchor_with_attributes_list),
                                                                       prefix='{} - processing nodes:'.format(f))
                                # for node_anchor in node_anchor_with_attributes_list:
                                #     node_anchor['properties'] = {}
                                for key in list(hmc_json.keys()):
                                    if isinstance(hmc_json[key], list):
                                        for hmc_json_element in hmc_json[key]:
                                            if hmc_json_element.get('nodeAnchorIndex'):
                                                topology_anchor_attribute_mapping(key, 'nodeAnchorIndex')
                                node_anchor_with_topology_list = []
                                for node_anchor_with_attributes in node_anchor_with_attributes_list:
                                    node_process_progressbar.update(node_anchor_with_attributes_index)
                                    node_anchor_with_attributes_index += 1
                                    node_ref = node_anchor_with_attributes['nodeRef']
                                    node_ref_partition_name = node_ref['partitionName']
                                    node_ref_identifier = node_ref['identifier']
                                    for feature in topology_geometry_reference_node_list['features']:
                                        if node_ref_identifier == feature['properties']['identifier']:
                                            node_anchor_geojson_feature = geojson.Feature()
                                            node_anchor_geojson_feature.geometry = geojson.geometry.Point(
                                                feature.geometry)
                                            node_anchor_geojson_feature.properties = node_anchor_with_attributes[
                                                'properties']
                                            node_anchor_with_topology_list.append(node_anchor_geojson_feature)
                                node_process_progressbar.finish()
                                node_anchor_with_topology_feature_collection = geojson.FeatureCollection(
                                    node_anchor_with_topology_list)
                                final_feature_collection.append(node_anchor_with_topology_feature_collection)
                                output_geojson_file_path.write(
                                    json.dumps(geojson.FeatureCollection(final_feature_collection), indent='    '))
