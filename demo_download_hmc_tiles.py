import json
import here.geotiles.heretile as heretile
from here.geotiles.heretile import BoundingBox, GeoCoordinate
from here.platform import Platform
from download_options import FileFormat, HerePlatformCatalog
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
                heretile.from_coordinates(self.download_target.lng, self.download_target.lat, level)]
        elif isinstance(self.download_target, BoundingBox):
            self.here_quad_longkey_list = list(heretile.in_bounding_box(
                self.download_target.west, self.download_target.south,
                self.download_target.east, self.download_target.north, level))
        elif isinstance(self.download_target, list):
            self.here_quad_longkey_list = self.download_target
        elif isinstance(self.download_target, tuple):
            self.get_tile_ids_by_country()

    def get_tile_ids_by_country(self):
        if not self.country_list_tuple:
            print('No country list provided, skipping country-based tile ID retrieval.')
            return

        indexed_locations_layer = 'indexed-locations'
        tile_id_list_per_country = HmcDownloader(
            catalog=self.catalog, layer=indexed_locations_layer, file_format=FileFormat.JSON
        ).get_country_tile_indexes(self.country_list_tuple)
        for tile_id_list in tile_id_list_per_country:
            self.here_quad_longkey_list.append(tile_id_list)


class LayerDownloader:
    def __init__(self, platform, catalog):
        self.platform = platform
        self.catalog = catalog
        self.available_layers = []

    def fetch_available_layers(self):
        """
        獲取可用的圖層列表，並返回這些圖層。
        """
        catalog_details = json.loads(json.dumps(self.catalog.get_details()))
        catalog_layers = catalog_details['layers']
        print('Available layers: ')
        self.available_layers = []
        for layer in catalog_layers:
            print('* {} | {} | {} | {}'.format(layer['id'], layer['name'], layer['hrn'], layer['tags']))
            if layer['partitioningScheme'] == 'heretile':
                self.available_layers.append({'layer_id': layer['id'], 'tiling_scheme': layer['partitioningScheme']})
        return self.available_layers  # 返回可用圖層列表

    def download_layers(self, layers_to_download, here_quad_longkey_list):
        """
        下載選定的圖層。

        :param layers_to_download: 要下載的圖層列表
        :param here_quad_longkey_list: 要下載的 TILE ID 列表
        """
        if not layers_to_download:
            print('No layers specified for download.')
            return

        print('Downloading layers:', layers_to_download)
        for layer in layers_to_download:
            print('* Downloading {}'.format(layer['layer_id']))
            downloader = HmcDownloader(catalog=self.catalog, layer=layer['layer_id'], file_format=FileFormat.JSON)
            downloader.download_partitioned_layer(quad_ids=here_quad_longkey_list)
            if downloader.get_schema():
                print('* Schema: {}'.format(downloader.get_schema().schema_hrn))
        print('Download complete.')


def main():
    platform = Platform()
    print('HERE Platform Status:', platform.get_status())

    download_center = GeoCoordinate(lng=13.059386842199105, lat=52.40345205930941)
    download_bounding_box = BoundingBox(west=97.73522936323553, south=9.591465308256108,
                                        east=106.08727704044883, north=20.981253503936394)
    download_quad_id_list = [23642688]
    country_list_tuple = None

    # 決定下載圖層的範圍或目標
    download_target = download_quad_id_list
    catalog = HerePlatformCatalog.HMC_EXT_REF_2

    hrn_map = {
        HerePlatformCatalog.HMC_RIB_2: ('hrn:here:data::olp-here:rib-2', 12),
        HerePlatformCatalog.HDLM_WEU_2: ('hrn:here:data::olp-here-had:here-hdlm-protobuf-weu-2', 14),
        HerePlatformCatalog.HMC_EXT_REF_2: ('hrn:here:data::olp-here:rib-external-references-2', 12)
    }

    hrn, level = hrn_map[catalog]
    platform_catalog = platform.get_catalog(hrn=hrn)

    geo_query = GeoQuery(platform_catalog, download_target, country_list_tuple)
    geo_query.resolve_tile_ids(level)

    if not geo_query.here_quad_longkey_list:
        print('No tile/partition ID presented, quit.')
        return

    # 下載圖層的流程
    layer_downloader = LayerDownloader(platform, platform_catalog)

    # 下載catalog中所有的圖層
    # available_layers = layer_downloader.fetch_available_layers()

    # 手動選定圖層
    available_layers = []

    hmc_rib_2_layers = [
        {'layer_id': 'address-locations', 'tiling_scheme': 'heretile'},
        {'layer_id': 'building-footprints', 'tiling_scheme': 'heretile'},
        {'layer_id': '3d-buildings', 'tiling_scheme': 'heretile'},
        {'layer_id': 'cartography', 'tiling_scheme': 'heretile'},
        {'layer_id': 'traffic-patterns', 'tiling_scheme': 'heretile'},
        {'layer_id': 'lane-attributes', 'tiling_scheme': 'heretile'},
        {'layer_id': 'address-attributes', 'tiling_scheme': 'heretile'},
        {'layer_id': 'adas-attributes', 'tiling_scheme': 'heretile'},
        {'layer_id': 'road-attributes', 'tiling_scheme': 'heretile'},
        {'layer_id': 'topology-geometry', 'tiling_scheme': 'heretile'},
        {'layer_id': 'navigation-attributes', 'tiling_scheme': 'heretile'},
        {'layer_id': 'advanced-navigation-attributes', 'tiling_scheme': 'heretile'},
        {'layer_id': 'truck-attributes', 'tiling_scheme': 'heretile'},
        {'layer_id': 'here-places', 'tiling_scheme': 'heretile'},
        {'layer_id': 'distance-markers', 'tiling_scheme': 'heretile'},
        {'layer_id': 'sign-text', 'tiling_scheme': 'heretile'},
        {'layer_id': 'here-places-essential-map', 'tiling_scheme': 'heretile'},
        {'layer_id': 'landmarks-3d', 'tiling_scheme': 'heretile'},
        {'layer_id': 'landmarks-2d', 'tiling_scheme': 'heretile'},
        {'layer_id': 'postal-code-points', 'tiling_scheme': 'heretile'},
        {'layer_id': 'postal-area-boundaries', 'tiling_scheme': 'heretile'},
        {'layer_id': 'electric-vehicle-charging-stations', 'tiling_scheme': 'heretile'},
        {'layer_id': 'electric-vehicle-charging-locations', 'tiling_scheme': 'heretile'},
        {'layer_id': 'here-truck-service-locations', 'tiling_scheme': 'heretile'},
        {'layer_id': 'here-fueling-stations', 'tiling_scheme': 'heretile'},
        {'layer_id': 'generalized-junctions-signs', 'tiling_scheme': 'heretile'},
        {'layer_id': 'enhanced-buildings', 'tiling_scheme': 'heretile'},
        {'layer_id': 'parking-areas', 'tiling_scheme': 'heretile'},
        {'layer_id': 'annotations', 'tiling_scheme': 'heretile'},
        {'layer_id': 'bicycle-attributes', 'tiling_scheme': 'heretile'},
        {'layer_id': 'warning-locations', 'tiling_scheme': 'heretile'},
        {'layer_id': 'complex-road-attributes', 'tiling_scheme': 'heretile'},
        {'layer_id': 'recreational-vehicle-attributes', 'tiling_scheme': 'heretile'},
    ]

    hdlm_weu_layers = [
        {'layer_id': 'localization-sign', 'tiling_scheme': 'heretile'},
        {'layer_id': 'speed-attributes', 'tiling_scheme': 'heretile'},
        {'layer_id': 'localization-road-surface-marking', 'tiling_scheme': 'heretile'},
        {'layer_id': 'routing-lane-attributes', 'tiling_scheme': 'heretile'},
        {'layer_id': 'localization-pole', 'tiling_scheme': 'heretile'},
        {'layer_id': 'administrative-areas', 'tiling_scheme': 'heretile'},
        {'layer_id': 'lane-topology', 'tiling_scheme': 'heretile'},
        {'layer_id': 'lane-geometry-polyline', 'tiling_scheme': 'heretile'},
        {'layer_id': 'routing-attributes', 'tiling_scheme': 'heretile'},
        {'layer_id': 'localization-barrier', 'tiling_scheme': 'heretile'},
        {'layer_id': 'localization-traffic-signal', 'tiling_scheme': 'heretile'},
        {'layer_id': 'topology-geometry', 'tiling_scheme': 'heretile'},
        {'layer_id': 'lane-road-references', 'tiling_scheme': 'heretile'},
        {'layer_id': 'state', 'tiling_scheme': 'heretile'},
        {'layer_id': 'localization-overhead-structure-face', 'tiling_scheme': 'heretile'},
        {'layer_id': 'external-reference-attributes', 'tiling_scheme': 'heretile'},
        {'layer_id': 'adas-attributes', 'tiling_scheme': 'heretile'},
        {'layer_id': 'lane-attributes', 'tiling_scheme': 'heretile'},
    ]

    hmc_external_references_layers = [
        {'layer_id': 'external-reference-attributes', 'tiling_scheme': 'heretile'},
    ]

    # 將圖層選項加入到可用圖層中
    if catalog == HerePlatformCatalog.HMC_RIB_2:
        available_layers.extend(hmc_rib_2_layers)
    elif catalog == HerePlatformCatalog.HDLM_WEU_2:
        available_layers.extend(hdlm_weu_layers)
    elif catalog == HerePlatformCatalog.HMC_EXT_REF_2:
        available_layers.extend(hmc_external_references_layers)

    # 假設用戶選擇要下載的圖層（這裡可以替換成實際用戶輸入的邏輯）
    layers_to_download = available_layers  # 假設選擇所有層進行下載
    layer_downloader.download_layers(layers_to_download, geo_query.here_quad_longkey_list)


if __name__ == '__main__':
    main()
