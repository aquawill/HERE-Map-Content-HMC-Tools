import json
import os
import re

import geojson


def geojson_file_reader(path, layer_name):
    file_path = ''
    for r, d, fs in os.walk(path):
        for f in fs:
            if re.match("{}_.*\.geojson$".format(layer_name), f):
                file_path = os.path.join(path, f)
                break
    if file_path != '':
        with open(file_path, 'r', encoding='utf-8') as attribute_reference_file:
            return geojson.loads(attribute_reference_file.read())
    else:
        return None


def json_file_reader(path, layer_name):
    file_path = ''
    for r, d, fs in os.walk(path):
        for f in fs:
            if re.match("{}_.*\.json$".format(layer_name), f):
                file_path = os.path.join(path, f)
                break
    if file_path != '':
        with open(file_path, 'r', encoding='utf-8') as attribute_reference_file:
            return json.loads(attribute_reference_file.read())


def segment_list_generator(partition_folder_path):
    topology_geometry_reference_geojson = geojson_file_reader(partition_folder_path, 'topology-geometry')
    if topology_geometry_reference_geojson:
        for topology_geometry_reference_geojson_feature_list in topology_geometry_reference_geojson['features']:
            if topology_geometry_reference_geojson_feature_list['properties'][0]['featureType'] == 'segment':
                return topology_geometry_reference_geojson_feature_list
    else:
        return None


def node_list_generator(partition_folder_path):
    topology_geometry_reference_geojson = geojson_file_reader(partition_folder_path, 'topology-geometry')
    if topology_geometry_reference_geojson:
        for topology_geometry_reference_geojson_feature_list in topology_geometry_reference_geojson['features']:
            if topology_geometry_reference_geojson_feature_list['properties'][0]['featureType'] == 'node':
                return topology_geometry_reference_geojson_feature_list
    else:
        return None


def named_place_list_generator(partition_folder_path):
    administrative_place_reference_json = json_file_reader(partition_folder_path, 'administrative-places')
    places = administrative_place_reference_json['place']
    del administrative_place_reference_json['place']
    for key in list(administrative_place_reference_json.keys()):
        if isinstance(administrative_place_reference_json[key], list) and administrative_place_reference_json[key][
            0].get('placeIndex') is not None:
            attribute_list = administrative_place_reference_json[key]
            for attribute in attribute_list:
                attribute_place_index = attribute['placeIndex']
                del attribute['placeIndex']
                for attribute_place_index in attribute_place_index:
                    places[attribute_place_index][key] = attribute
            del administrative_place_reference_json[key]
    return places
