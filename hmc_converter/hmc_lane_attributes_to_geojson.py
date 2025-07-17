import json
import os
import re

import geojson
import shapely.ops
from progressbar import ProgressBar

import hmc_layer_cross_referencing


def topology_anchor_multi_attribute_mapping(feature_list, attribute_name, attribute_index):
    attribute_list = hmc_json[attribute_name]
    attribute_progressbar = ProgressBar(min_value=0, max_value=len(
        attribute_list), prefix='{} - processing {}:'.format(f, attribute_name))
    attribute_index = 0
    for attribute in attribute_list:
        attribute_progressbar.update(attribute_index)
        attribute_index += 1
        if attribute.get(attribute_index):
            if feature_list:
                attribute_from_attribute_indexes = attribute.get(attribute_index)
                del attribute[attribute_index]
                for attribute_from_attribute_index in attribute_from_attribute_indexes:
                    feature_list[attribute_from_attribute_index]['properties'].append(
                        {attribute_name: attribute})
    attribute_progressbar.finish()


input_layers = ['lane-attributes']

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('partition_path', help='path of partition folder', type=str)
    parser.add_argument('overwrite_result', help='overwrite geojson result file (y/N)', nargs='?', default='n',
                        type=str)
    args = parser.parse_args()
    partition_folder_path = args.partition_path
    overwrite_result = str.lower(args.overwrite_result)

    # hmc_external_reference = HMCExternalReferences()

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
                        lane_list = hmc_json.get('lane')
                        lane_anchor_list = hmc_json.get('laneAnchor')

                        if segment_anchor_with_attributes_list:
                            segment_output_geojson_file_path = os.path.join(r, '{}_segments.geojson'.format(f))
                            if os.path.exists(segment_output_geojson_file_path) and overwrite_result != 'y':
                                print('{} --> existing already.'.format(segment_output_geojson_file_path))
                            else:
                                topology_geometry_reference_segment_list = hmc_layer_cross_referencing.segment_list_generator(
                                    r)
                                with open(segment_output_geojson_file_path, mode='w',
                                          encoding='utf-8') as segment_output_geojson_file_path:
                                    segment_anchor_with_attributes_index = 0

                                    for segment_anchor in segment_anchor_with_attributes_list:
                                        segment_anchor['properties'] = []

                                    # lane strand mapping
                                    lane_anchor_process_progressbar = ProgressBar(min_value=0, max_value=len(
                                        lane_list), prefix='{} - processing lane anchor:'.format(f))
                                    lane_anchor_index = 0
                                    for lane in lane_list:
                                        lane_anchor_process_progressbar.update(lane_anchor_index)
                                        lane_anchor_index += 1
                                        lane['properties'] = []
                                        lane['laneStrand'] = []
                                        for lane_anchor in lane_anchor_list:
                                            lane_anchor_lane_strand = lane_anchor['laneStrand']
                                            for lane_ref_list in lane_anchor_lane_strand:
                                                for lane_ref in lane_ref_list['laneRef']:
                                                    if lane_ref['identifier'] == lane['identifier']:
                                                        lane['laneStrand'].append(lane_anchor_lane_strand)
                                    lane_anchor_process_progressbar.finish()

                                    # lane attribute mapping
                                    for key in list(hmc_json.keys()):
                                        if len(hmc_json[key]) > 0:
                                            if isinstance(hmc_json[key], list) and hmc_json[key][0].get('laneIndex'):
                                                topology_anchor_multi_attribute_mapping(lane_list, key, 'laneIndex')

                                    # segment lane mapping
                                    for key in list(hmc_json.keys()):
                                        if len(hmc_json[key]) > 0:
                                            if isinstance(hmc_json[key], list) and hmc_json[key][0].get(
                                                    'segmentAnchorIndex'):
                                                topology_anchor_multi_attribute_mapping(
                                                    segment_anchor_with_attributes_list,
                                                    key, 'segmentAnchorIndex')

                                    # segment topology mapping
                                    segment_process_progressbar = ProgressBar(min_value=0, max_value=len(
                                        segment_anchor_with_attributes_list), prefix='{} - processing segments:'.format(
                                        f))
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
                                                        segment_anchor_geojson_feature.type = 'Feature'
                                                        segment_anchor_geojson_feature.geometry = geojson.geometry.Point(
                                                            feature_geometry_with_offsets_geojson)
                                                    else:
                                                        segment_anchor_geojson_feature.type = 'Feature'
                                                        segment_anchor_geojson_feature.geometry = geojson.geometry.LineString(
                                                            feature_geometry_with_offsets_geojson)

                                                        # Get LINK PVID with HMC Segment ID
                                                        # segment_anchor_geojson_feature.properties[
                                                        #     'hmcExternalReference'] = {}
                                                        # segment_anchor_geojson_feature.properties['hmcExternalReference'][
                                                        #     'pvid'] = hmc_external_reference.segment_to_pvid(
                                                        #     partition_id=partition_name,
                                                        #     segment_ref=Ref(partition=Partition(str(partition_name)),
                                                        #                     identifier=Identifier(
                                                        #                         segment_ref['identifier'])))

                                                    segment_anchor_with_topology_list.append(
                                                        segment_anchor_geojson_feature)
                                    segment_anchor_with_topology_feature_collection = geojson.FeatureCollection(
                                        segment_anchor_with_topology_list)
                                    segment_process_progressbar.finish()
                                    segment_output_geojson_file_path.write(
                                        json.dumps(segment_anchor_with_topology_feature_collection, indent='    '))
