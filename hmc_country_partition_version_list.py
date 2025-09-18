import json

from google.protobuf.json_format import MessageToJson
from here.platform import Platform
from here.platform.adapter import DecodedMessage

rib_2_hrn = 'hrn:here:data::olp-here:rib-2'
platform = Platform()
rib_2_catalog = platform.get_catalog(hrn=rib_2_hrn)
indexed_locations_layer = rib_2_catalog.get_layer('indexed-locations')
target_layer = rib_2_catalog.get_layer('topology-geometry')
partitions = list(indexed_locations_layer.read_partitions(['SGP']))
for p in partitions:
    partition, content = p
    decoded_content = DecodedMessage(content)
    decoded_content_json = json.loads(MessageToJson(decoded_content))
    tile_id_list = decoded_content_json.get('tileId')
    topology_geometry_partitions = list(target_layer.read_partitions(tile_id_list))
    for topology_geometry_p in topology_geometry_partitions:
        topology_geometry_partition, topology_geometry_content = topology_geometry_p
        print('layer: {} | id: {} | version: {}'.format(target_layer.id, topology_geometry_partition.id, topology_geometry_partition.version))
