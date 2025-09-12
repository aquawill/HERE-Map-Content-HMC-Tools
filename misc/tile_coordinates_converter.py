import argparse
import json

import here.geotiles.heretile as heretile


def tile_xy_to_wgs84(x, y, tile_row, tile_column, tile_level, world_coordinate_bits):
    i_lat_tile = tile_row << (world_coordinate_bits - tile_level)
    i_lng_tile = tile_column << (world_coordinate_bits - tile_level)

    i_lat = i_lat_tile + y
    i_lng = i_lng_tile + x

    lat = (i_lat * 360.0) / (1 << world_coordinate_bits) - 90.0
    lng = (i_lng * 360.0) / (1 << world_coordinate_bits) - 180.0

    return lat, lng


def wgs84_to_tile_xy(lat, lng, tile_level, world_coordinate_bits):
    i_lat = int(((lat + 90.0) / 360.0) * (1 << world_coordinate_bits))
    i_lng = int(((lng + 180.0) / 360.0) * (1 << world_coordinate_bits))

    shift = world_coordinate_bits - tile_level
    tile_row = i_lat >> shift
    tile_column = i_lng >> shift

    y = i_lat - (tile_row << shift)
    x = i_lng - (tile_column << shift)

    tile = heretile.from_x_y_level(tile_column, tile_row, tile_level)
    return tile, tile_row, tile_column, x, y


def quadkey_xy_to_wgs84(quadkey, x, y, world_coordinate_bits):
    tile_column, tile_row, tile_level = heretile.get_x_y_level(quadkey)
    lat, lng = tile_xy_to_wgs84(x, y, tile_row, tile_column, tile_level, world_coordinate_bits)
    return {
        "lat": lat,
        "lng": lng,
        "tile_level": tile_level,
        "tile_row": tile_row,
        "tile_column": tile_column
    }


def main():
    parser = argparse.ArgumentParser(description="Tile ⇄ WGS84 CLI Tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Mode 1: WGS84 → tile
    p1 = subparsers.add_parser("from-wgs84-to-tile-coords", help="Convert WGS84 to tile and inner xy")
    p1.add_argument("--lat", type=float, required=True)
    p1.add_argument("--lng", type=float, required=True)
    p1.add_argument("--level", type=int, required=True)
    p1.add_argument("--world_coordinate_bits", type=int, required=True)


    # Mode 2: tile XY → WGS84
    p2 = subparsers.add_parser("from-tile-coords-to-wgs84", help="Convert tile XY to WGS84")
    p2.add_argument("--tile_column", type=int, required=True)
    p2.add_argument("--tile_row", type=int, required=True)
    p2.add_argument("--x", type=int, required=True)
    p2.add_argument("--y", type=int, required=True)
    p2.add_argument("--level", type=int, required=True)
    p2.add_argument("--world_coordinate_bits", type=int, required=True)

    # Mode 3: quadkey + inner XY → WGS84
    p3 = subparsers.add_parser("from-quadkey-coords-to-wgs84", help="Convert quadkey + tile xy to WGS84")
    p3.add_argument("--quadkey", type=int, required=True)
    p3.add_argument("--x", type=int, required=True)
    p3.add_argument("--y", type=int, required=True)
    p3.add_argument("--world_coordinate_bits", type=int, required=True)

    # Mode 4: quadkey → tile row/column/level
    p4 = subparsers.add_parser("from-quadkey-to-tile", help="Convert quadkey to tile row/column/level")
    p4.add_argument("--quadkey", type=int, required=True)

    # Mode 5: tile row/column/level → quadkey
    p5 = subparsers.add_parser("from-tile-to-quadkey", help="Convert tile row/column/level to quadkey")
    p5.add_argument("--tile_column", type=int, required=True)
    p5.add_argument("--tile_row", type=int, required=True)
    p5.add_argument("--level", type=int, required=True)
    p5.add_argument("--world_coordinate_bits", type=int, required=True)

    args = parser.parse_args()

    if args.command == "from-wgs84-to-tile-coords":
        tile, row, col, x, y = wgs84_to_tile_xy(args.lat, args.lng, args.level, args.world_coordinate_bits)
        result = {
            "quadkey": tile,
            "tile_row": row,
            "tile_column": col,
            "tile_inner_x": x,
            "tile_inner_y": y
        }

    elif args.command == "from-tile-coords-to-wgs84":
        lat, lng = tile_xy_to_wgs84(args.x, args.y, args.tile_row, args.tile_column, args.level, args.world_coordinate_bits)
        result = {
            "lat": lat,
            "lng": lng
        }

    elif args.command == "from-quadkey-coords-to-wgs84":
        result = quadkey_xy_to_wgs84(args.quadkey, args.x, args.y, args.world_coordinate_bits)

    elif args.command == "from-quadkey-to-tile":
        tile_column, tile_row, tile_level = heretile.get_x_y_level(args.quadkey)
        result = {
            "tile_row": tile_row,
            "tile_column": tile_column,
            "tile_level": tile_level
        }

    elif args.command == "from-tile-to-quadkey":
        tile = heretile.from_x_y_level(args.tile_column, args.tile_row, args.level)
        result = {
            "quadkey": tile
        }

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
