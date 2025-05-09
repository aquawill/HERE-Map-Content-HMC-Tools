import argparse
import yaml
import json
from here.platform import Platform
from here.geotiles.heretile import GeoCoordinate, BoundingBox, in_bounding_box, from_coordinates

from hmc_downloader import HmcDownloader
from hmc_download_options import FileFormat, HerePlatformCatalog, DownloadMethod


CATALOG_HRN_LEVEL_MAP = {
    "HMC_RIB_2": ("hrn:here:data::olp-here:rib-2", 12),
    "HDLM_WEU_2": ("hrn:here:data::olp-here-had:here-hdlm-protobuf-weu-2", 14),
    "HMC_EXT_REF_2": ("hrn:here:data::olp-here:rib-external-references-2", 12),
}


class YamlBasedDownloader:
    def __init__(self, config_path: str):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

        self._validate_config()

        self.platform = Platform()
        self.catalog_enum = HerePlatformCatalog[self.config['catalog']]
        self.hrn, self.level = CATALOG_HRN_LEVEL_MAP[self.config['catalog']]
        self.catalog = self.platform.get_catalog(self.hrn)
        self.version = self.config.get('version')
        self.method = DownloadMethod[self.config['download_method']]
        self.target = self.config['target']
        self.layers = self.config.get('layers', [])

    def _validate_config(self):
        required_fields = ["catalog", "download_method", "target", "layers"]
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required field: {field}")
        if "type" not in self.config["target"]:
            raise ValueError("Missing 'type' field in target")

    def resolve_tile_ids(self):
        query_type = self.target['type']
        if query_type == 'coordinate':
            coord = self.target['coordinate']
            return [from_coordinates(coord['lng'], coord['lat'], self.level)]
        elif query_type == 'bounding_box':
            box = self.target['bounding_box']
            return list(in_bounding_box(
                box['west'], box['south'], box['east'], box['north'], self.level))
        elif query_type == 'tile_id_list':
            return self.target['tile_ids']
        elif query_type == 'country':
            countries = tuple(self.target['countries'])
            downloader = HmcDownloader(
                catalog=self.catalog,
                layer='indexed-locations',
                file_format=FileFormat.JSON,
            )
            result = downloader.get_country_tile_indexes(countries)
            tile_ids = []
            for tile_dict in result:
                for tile_list in tile_dict.values():
                    tile_ids.extend(tile_list)
            return tile_ids
        else:
            raise ValueError(f"Unsupported query type: {query_type}")

    def run(self):
        quad_ids = self.resolve_tile_ids()
        if not quad_ids:
            print("No partition IDs resolved.")
            return

        for layer_id in self.layers:
            print(f"Downloading {layer_id}...")
            downloader = HmcDownloader(
                catalog=self.catalog,
                layer=layer_id,
                file_format=FileFormat.JSON,
                version=self.version,
            )
            if self.method == DownloadMethod.DATA_SDK:
                downloader.download_partitioned_layer(quad_ids, write_to_file=True, version=self.version)
            elif self.method == DownloadMethod.OLP_CLI:
                downloader.olp_cli_download_partition('heretile', quad_ids, write_to_file=True, version=self.version)
            if downloader.get_schema():
                print("  Schema:", downloader.get_schema().schema_hrn)
        print("✅ All downloads complete.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Download HERE HMC layers using YAML config")
    parser.add_argument('--config', required=True, help='Path to YAML config file')
    parser.add_argument('--init', action='store_true', help='Generate sample config.yaml and exit')
    args = parser.parse_args()

    if args.init:
        sample = {
            'catalog': 'HMC_RIB_2',
            'version': None,
            'download_method': 'OLP_CLI',
            'target': {
                'type': 'bounding_box',
                'bounding_box': {
                    'west': 97.735,
                    'south': 9.591,
                    'east': 106.087,
                    'north': 20.981
                }
            },
            'layers': [
                'adas-attributes',
                'topology-geometry',
                'navigation-attributes',
                'advanced-navigation-attributes'
            ]
        }
        with open('config.yaml', 'w') as f:
            yaml.dump(sample, f)
        print("✅ Sample config.yaml created.")
    else:
        tool = YamlBasedDownloader(args.config)
        tool.run()
