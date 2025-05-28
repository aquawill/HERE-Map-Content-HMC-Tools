from google.protobuf.json_format import MessageToJson
from here.platform import Platform
from here.platform.adapter import DecodedMessage
from here.platform.catalog import Catalog

# version = 6955

platform = Platform()
catalog = Catalog(catalog_hrn, platform)
layer = catalog.get_layer(layer_hrn)
partitions = layer.read_partitions(partition_list)
for partition in partitions:
    versioned_partition, partition_content = partition
    print(versioned_partition.id)
    decoded = DecodedMessage(partition_content)
    with open(f'{layer_hrn}_{partition_list}_decoded.txt', mode='w', encoding='utf-8') as decoded_output_file:
        decoded_output_file.write(str(decoded))

    json = MessageToJson(decoded)
    with open(f'{layer_hrn}_{partition_list}_decoded.json', mode='w', encoding='utf-8') as json_output_file:
        json_output_file.write(str(json))
