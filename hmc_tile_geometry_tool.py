import shapely
from mapquadlib.herequad import HereQuad


def convert(latitude, longitude, tile_id, here_platform_catalog, result_mode):
    if here_platform_catalog == 'hdlm':
        zoom = 14
    elif here_platform_catalog == 'rib2':
        zoom = 12
    else:
        print('catalog should be "hdlm" or "rib2"')
        quit()

    if latitude and longitude:
        quad = HereQuad.from_lat_lng_level(latitude, longitude, zoom)
    elif tile_id:
        quad = HereQuad.from_long_key(tile_id)
    else:
        print('please input lat/lng or tile id, such as:\n'
              'python hmc_tile_geometry_tool.py --lng 52.52507 --lat 13.36937 --catalog rib2 --mode polygon\n'
              'python hmc_tile_geometry_tool.py --id 24319681 --catalog rib2 --mode polyline\n')
        quit()

    result = None

    if str(result_mode).lower() == 'id':
        result = quad.long_key
    elif str(result_mode).lower() == 'bbox':
        result = HereQuad.from_quad_key(quad.quad_key).bounding_box
    elif str(result_mode).lower() == 'polygon':
        result = HereQuad.from_quad_key(quad.quad_key).bounding_box.geojson
    elif str(result_mode).lower() == 'polyline':
        result = shapely.to_geojson(
            shapely.from_geojson(HereQuad.from_quad_key(quad.quad_key).bounding_box.geojson).boundary)

    print(result)
    return result


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument('-lng', '--longitude', help='longitude, such as 52.52507', type=float, nargs='?')
    parser.add_argument('-lat', '--latitude', help='latitude, such as 13.36937', type=float, nargs='?')
    parser.add_argument('-id', '--tileid', help='tile/partition id, such as 24319681', type=int, nargs='?')
    parser.add_argument('-c', '--catalog', help='"hdlm" or "rib2"', type=str, choices=['hdlm', 'rib2'])
    parser.add_argument('-m', '--mode', type=str, choices=['id', 'bbox', 'polygon', 'polyline'])
    args = parser.parse_args()

    lat = args.latitude
    lng = args.longitude
    tileid = args.tileid
    catalog = args.catalog
    mode = args.mode

    convert(lat, lng, tileid, catalog, mode)
