import argparse

import here.geotiles.heretile as heretile
import shapely
from progressbar import ProgressBar

# input_geojson_file = 'thailand.geojson'
parser = argparse.ArgumentParser()

parser.add_argument('input_geojson_file_path', help='path of input geojson file', type=str)
parser.add_argument('level', help='level, 12 for rib-2 and 14 for hdlm', type=int)

args = parser.parse_args()
input_geojson_file = args.input_geojson_file_path
level = args.level


def get_size_of_iterator(iterator) -> int:
    size = 0
    for e in iterator:
        size += 1
    return size


def get_list_from_iterator(iterator) -> list:
    output_list = []
    for e in iterator:
        output_list.append(e)
    return output_list


tile_count = 0

with open('{}_TILE_LIST.txt'.format(input_geojson_file), 'w') as tile_list_output_file:
    tile_list_output_file.write('quadkey\twkt\n')
    with open(input_geojson_file, 'r') as input_geojson:
        input_geojson_str = input_geojson.read()
        geometry_index = 0
        west, south, east, north = shapely.from_geojson(input_geojson_str).bounds
        tile_list_in_bounding_box = heretile.in_bounding_box(west=west, south=south, east=east, north=north,
                                                             level=level)
        tile_list_in_bounding_box = get_list_from_iterator(tile_list_in_bounding_box)
        tile_count = len(tile_list_in_bounding_box)
        tile_index = 0

        progressbar = ProgressBar(max_value=tile_count, min_value=tile_index,
                                  prefix='{}    -->    ||'.format(input_geojson.name),
                                  suffix='    ||   -->    {}'.format(tile_list_output_file.name))

        for tile in tile_list_in_bounding_box:
            progressbar.update(tile_index)
            tile_index += 1
            (tile_west, tile_south, tile_east, tile_north) = heretile.get_bounds(tile)
            tile_polygon = shapely.Polygon.from_bounds(tile_west, tile_south, tile_east, tile_north)
            if shapely.from_geojson(input_geojson_str).intersects(tile_polygon):
                tile_list_output_file.write('{}\t{}\n'.format(tile.real, tile_polygon.wkt))
        geometry_index += 1

# Example: python test_here_quad_list_from_geojson.py thailand.geojson 12
