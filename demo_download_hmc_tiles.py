"""
This script is a sample utility for downloading heretile-partitioned layers from the HERE Platform.

It provides multiple ways to define the geographic scope of data to be downloaded
and supports both HERE Data SDK and OLP CLI as download methods.

------------
Prerequisites:
- Python 3.7+
- Valid HERE Platform credentials (e.g., via environment variables or credentials file)
- Required Python modules:
    - here.geotiles
    - here.platform
    - hmc_downloader.py (custom logic for interacting with partitioned layers)
    - hmc_download_options.py (defines enums for catalogs, file format, and download method)
- If using `DownloadMethod.OLP_CLI`, OLP CLI must be installed and configured

------------
Download Target Configuration:
In the `main()` function, choose one of the following options and assign it to the variable `download_target`.

1. **GeoCoordinate** (`download_center`)
   Downloads the tile that contains a single lat/lon point.
   Example:
       download_center = GeoCoordinate(lat=51.664415, lng=-3.80175)

2. **BoundingBox** (`download_bounding_box`)
   Downloads all tiles that intersect the given bounding box.
   Example:
       download_bounding_box = BoundingBox(west=97.73, south=9.59, east=106.08, north=20.98)

3. **Tile ID list** (`download_quad_id_list`)
   Downloads specific tile(s) by ID.
   Example:
       download_quad_id_list = [23611407]

4. **Country-based tile list** (`country_list_tuple`)
   Automatically resolves tile IDs by country ISO3 codes.
   Set `download_target = country_list_tuple` and define countries like:
       country_list_tuple = ("TWN", "PHL")

**Note: ** Only one of the above should be active at a time. Use `download_target` to select which target to download.

------------
Other Important Variables:

- `catalog`: Specifies which HERE catalog to use.
    Choose from:
      - HerePlatformCatalog.HMC_RIB_2
      - HerePlatformCatalog.HDLM_WEU_2
      - HerePlatformCatalog.HMC_EXT_REF_2

- `download_version`: Optional. Set to a specific catalog version (int), or None to use the latest.

- `download_method`: Choose download strategy:
    - DownloadMethod.DATA_SDK — Uses HERE Data SDK (requires credentials)
    - DownloadMethod.OLP_CLI — Uses OLP CLI (must be installed)

- `available_layers`: Optionally specify which layers to download manually.
    If left empty, all heretile layers from the selected catalog will be used.

------------
Execution:

After setting the appropriate options in `main()`, run the script:

    python demo_download_hmc_tiles.py

The script will:
- Print HERE Platform connection status
- Resolve partition (tile) IDs based on your selection
- Fetch available layers in the catalog
- Download the selected layers using the chosen method
- Display schema information if available

------------
Tips:
- Enable or disable layer sets in the `hmc_rib_2_layers`, `hdlm_weu_layers`, or `hmc_external_references_layers` lists.
- Add logging or output redirection if running in batch mode or automation.

"""

import json

import here.geotiles.heretile as heretile
from here.geotiles.heretile import BoundingBox, GeoCoordinate
from here.platform import Platform

from hmc_download_options import FileFormat, HerePlatformCatalog, DownloadMethod
from hmc_downloader import HmcDownloader


class GeoQuery:
    def __init__(self, catalog, download_target, country_list_tuple=None):
        self.catalog = catalog
        self.download_target = download_target
        self.country_list_tuple = country_list_tuple
        self.here_quad_longkey_list = []

    def resolve_tile_ids(self, level):
        if isinstance(self.download_target, GeoCoordinate):
            self.here_quad_longkey_list = [
                heretile.from_coordinates(
                    self.download_target.lng, self.download_target.lat, level
                )
            ]
        elif isinstance(self.download_target, BoundingBox):
            self.here_quad_longkey_list = list(
                heretile.in_bounding_box(
                    self.download_target.west,
                    self.download_target.south,
                    self.download_target.east,
                    self.download_target.north,
                    level,
                )
            )
        elif isinstance(self.download_target, list):
            self.here_quad_longkey_list = self.download_target
        elif isinstance(self.download_target, tuple):
            self.get_tile_ids_by_country()

    def get_tile_ids_by_country(self):
        if not self.country_list_tuple:
            print("No country list provided, skipping country-based tile ID retrieval.")
            return

        indexed_locations_layer = "indexed-locations"
        tile_id_list_per_country = HmcDownloader(
            catalog=self.catalog,
            layer=indexed_locations_layer,
            file_format=FileFormat.JSON,
        ).get_country_tile_indexes(self.country_list_tuple)
        for tile_id_list in tile_id_list_per_country:
            print("tile_id_list", tile_id_list)
            self.here_quad_longkey_list.append(tile_id_list)


class LayerDownloader:
    def __init__(self, platform, catalog, version, method):
        self.platform = platform
        self.catalog = catalog
        self.available_layers = []
        self.version = version
        self.method = method

    def fetch_available_layers(self):
        """
        取得可用的圖層列表，並返回這些圖層。
        """
        catalog_details = json.loads(json.dumps(self.catalog.get_details()))
        catalog_layers = catalog_details["layers"]
        print("Available layers: ")
        self.available_layers = []
        for layer in catalog_layers:
            print(
                "* {} | {} | {} | {}".format(
                    layer["id"], layer["name"], layer["hrn"], layer["tags"]
                )
            )
            if layer["partitioningScheme"] == "heretile":
                self.available_layers.append(
                    {
                        "layer_id": layer["id"],
                        "tiling_scheme": layer["partitioningScheme"],
                    }
                )
        return self.available_layers  # 返回可用圖層列表

    def download_layers(self, layers_to_download, here_quad_longkey_list):
        """
        下載選定的圖層。

        :param layers_to_download: 要下載的圖層列表
        :param here_quad_longkey_list: 要下載的 TILE ID 列表
        """
        if not layers_to_download:
            print("No layers specified for download.")
            return

        print("Downloading layers:", layers_to_download)
        for layer in layers_to_download:
            print("* Downloading {}".format(layer["layer_id"]))
            downloader = HmcDownloader(
                catalog=self.catalog,
                layer=layer["layer_id"],
                file_format=FileFormat.JSON,
                version=self.version,
            )

            if self.method == DownloadMethod.DATA_SDK:
                downloader.download_partitioned_layer(
                    quad_ids=here_quad_longkey_list,
                    write_to_file=True,
                    version=self.version,
                )

            if self.method == DownloadMethod.OLP_CLI:
                try:
                    downloader.olp_cli_download_partition(
                        tiling_scheme='heretile',
                        quad_ids=here_quad_longkey_list,
                        write_to_file=True,
                        version=self.version,
                    )
                except RuntimeError as e:
                    print(e)
                    pass

            if downloader.get_schema():
                print("Schema: {}".format(downloader.get_schema().schema_hrn))

        print("Download complete.")


def main():
    platform = Platform()
    print("HERE Platform Status:", platform.get_status())

    # 選項1：下載經緯度所在的partition
    download_center = GeoCoordinate(lat=41.1185338888889, lng=-8.62504861111111)

    # 選項2：下載bounding box所包含的partitions
    download_bounding_box = BoundingBox(
        west=97.73522936323553,
        south=9.591465308256108,
        east=106.08727704044883,
        north=20.981253503936394,
    )

    # 選項3：下載指定的partition ID
    download_quad_id_list = [23618402, 23618403]

    # 選項4：下載指定的的國家（使用ISO 3166-1 alpha-3編碼）
    country_list_tuple = None

    # 選項5：決定下載圖層的範圍或目標
    download_target = download_center

    # 選項6：決定下載圖層的最高版本 (int or None)
    download_version = None

    # 選項7：選擇要下載的catalog
    catalog = HerePlatformCatalog.HMC_RIB_2

    hrn_map = {
        HerePlatformCatalog.HMC_RIB_2: ("hrn:here:data::olp-here:rib-2", 12),
        HerePlatformCatalog.HDLM_WEU_2: (
            "hrn:here:data::olp-here-had:here-hdlm-protobuf-weu-2",
            14,
        ),
        HerePlatformCatalog.HMC_EXT_REF_2: (
            "hrn:here:data::olp-here:rib-external-references-2",
            12,
        ),
    }

    hrn, level = hrn_map[catalog]
    platform_catalog = platform.get_catalog(hrn=hrn)

    geo_query = GeoQuery(platform_catalog, download_target, country_list_tuple)
    geo_query.resolve_tile_ids(level)

    if not geo_query.here_quad_longkey_list:
        print("No tile/partition ID presented, quit.")
        return

    # 選擇下載使用Data SDK或是OLP CLI (OLP CLI 需要另外安裝)
    download_method = DownloadMethod.OLP_CLI

    # 下載圖層的流程
    layer_downloader = LayerDownloader(platform, platform_catalog, download_version, download_method)

    # 選項8：選擇要下載的圖層

    available_layers = []

    hmc_rib_2_layers = [
        # {'layer_id': 'address-locations', 'tiling_scheme': 'heretile'},
        # {'layer_id': 'building-footprints', 'tiling_scheme': 'heretile'},
        # {'layer_id': '3d-buildings', 'tiling_scheme': 'heretile'},
        # {'layer_id': 'cartography', 'tiling_scheme': 'heretile'},
        # {'layer_id': 'traffic-patterns', 'tiling_scheme': 'heretile'},
        # {'layer_id': 'lane-attributes', 'tiling_scheme': 'heretile'},
        # {'layer_id': 'address-attributes', 'tiling_scheme': 'heretile'},
        {'layer_id': 'adas-attributes', 'tiling_scheme': 'heretile'},
        # {'layer_id': 'road-attributes', 'tiling_scheme': 'heretile'},
        {"layer_id": "topology-geometry", "tiling_scheme": "heretile"},
        {"layer_id": "navigation-attributes", "tiling_scheme": "heretile"},
        {'layer_id': 'advanced-navigation-attributes', 'tiling_scheme': 'heretile'},
        # {'layer_id': 'truck-attributes', 'tiling_scheme': 'heretile'},
        # {'layer_id': 'places', 'tiling_scheme': 'heretile'},
        # {'layer_id': 'distance-markers', 'tiling_scheme': 'heretile'},
        # {'layer_id': 'sign-text', 'tiling_scheme': 'heretile'},
        # {'layer_id': 'postal-code-points', 'tiling_scheme': 'heretile'},
        # {'layer_id': 'postal-area-boundaries', 'tiling_scheme': 'heretile'},
        # {'layer_id': 'electric-vehicle-charging-stations', 'tiling_scheme': 'heretile'},
        # {'layer_id': 'electric-vehicle-charging-locations', 'tiling_scheme': 'heretile'},
        # {'layer_id': 'enhanced-buildings', 'tiling_scheme': 'heretile'},
        # {'layer_id': 'parking-areas', 'tiling_scheme': 'heretile'},
        # {'layer_id': 'annotations', 'tiling_scheme': 'heretile'},
        # {'layer_id': 'bicycle-attributes', 'tiling_scheme': 'heretile'},
        # {'layer_id': 'warning-locations', 'tiling_scheme': 'heretile'},
        # {'layer_id': 'complex-road-attributes', 'tiling_scheme': 'heretile'},
        # {'layer_id': 'recreational-vehicle-attributes', 'tiling_scheme': 'heretile'},
        # {'layer_id': 'buildings', 'tiling_scheme': 'heretile'}
    ]

    hdlm_weu_layers = [
        #     {'layer_id': 'administrative-areas', 'tiling_scheme': 'heretile'},
        #     {'layer_id': 'adas-attributes', 'tiling_scheme': 'heretile'},
        #     {'layer_id': 'external-reference-attributes', 'tiling_scheme': 'heretile'},
        #     {'layer_id': 'localization-sign', 'tiling_scheme': 'heretile'},
        #     {'layer_id': 'localization-road-surface-marking', 'tiling_scheme': 'heretile'},
        #     {'layer_id': 'localization-pole', 'tiling_scheme': 'heretile'},
        #     {'layer_id': 'lane-topology', 'tiling_scheme': 'heretile'},
        #     {'layer_id': 'lane-geometry-polyline', 'tiling_scheme': 'heretile'},
        #     {'layer_id': 'localization-barrier', 'tiling_scheme': 'heretile'},
        #     {'layer_id': 'localization-traffic-signal', 'tiling_scheme': 'heretile'},
        #     {'layer_id': 'lane-road-references', 'tiling_scheme': 'heretile'},
        #     {'layer_id': 'localization-overhead-structure-face', 'tiling_scheme': 'heretile'},
        #     {'layer_id': 'lane-attributes', 'tiling_scheme': 'heretile'},
        #     {'layer_id': 'routing-lane-attributes', 'tiling_scheme': 'heretile'},
        #     {'layer_id': 'routing-attributes', 'tiling_scheme': 'heretile'},
        #     {'layer_id': 'speed-attributes', 'tiling_scheme': 'heretile'},
        #     {'layer_id': 'state', 'tiling_scheme': 'heretile'},
        #     {'layer_id': 'topology-geometry', 'tiling_scheme': 'heretile'},
    ]

    hmc_external_references_layers = [
        #     {'layer_id': 'external-reference-attributes', 'tiling_scheme': 'heretile'},
    ]

    # 將圖層選項加入到可用圖層中
    if catalog == HerePlatformCatalog.HMC_RIB_2:
        if len(hmc_rib_2_layers) > 0:
            available_layers.extend(hmc_rib_2_layers)
    elif catalog == HerePlatformCatalog.HDLM_WEU_2:
        if len(hdlm_weu_layers) > 0:
            available_layers.extend(hdlm_weu_layers)
    elif catalog == HerePlatformCatalog.HMC_EXT_REF_2:
        if len(hmc_external_references_layers) > 0:
            available_layers.extend(hmc_external_references_layers)

    # 開始下載
    if len(available_layers) > 0:
        layers_to_download = available_layers
    else:
        layers_to_download = layer_downloader.fetch_available_layers()
    layer_downloader.download_layers(
        layers_to_download, geo_query.here_quad_longkey_list
    )


if __name__ == "__main__":
    main()
