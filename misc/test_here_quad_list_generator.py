import argparse

import here.geotiles.heretile as heretile

parser = argparse.ArgumentParser()

parser.add_argument('west', help='longitude of west, 0~-180', type=float)
parser.add_argument('south', help='latitude of south, 0~-90', type=float)
parser.add_argument('east', help='longitude of east, 0~180', type=float)
parser.add_argument('north', help='latitude of north, 0~90', type=float)
parser.add_argument('level', help='level, 12 for rib-2 and 14 for hdlm', type=int)
parser.add_argument('output_file', help='path of output file', type=str)

args = parser.parse_args()

tile_list = heretile.in_bounding_box(west=args.west, south=args.south, east=args.east, north=args.north,
                                     level=args.level)

with open(args.output_file, 'w') as output:
    output.write('quadkey\twkt\n')
    for tile in tile_list:
        (west, south, east, north) = heretile.get_bounds(tile)
        wkt = heretile.BoundingBox(west, south, east, north).wkt
        output.write('{}\t{}\n'.format(tile.real, wkt))

# Example: python test_here_quad_list_generator.py -1 -1 1 1 12 testlist.txt
# Example: python test_here_quad_list_generator.py -180 -90 180 90 12 fulllist.txt


