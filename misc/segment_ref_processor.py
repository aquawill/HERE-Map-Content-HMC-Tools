from here.content.utils.hmc_external_references import HMCExternalReferences
from here.platform.adapter import Identifier
from here.platform.adapter import Partition
from here.platform.adapter import Ref
from mapquadlib.herequad import HereQuad

hmc_external_reference = HMCExternalReferences()

segment_ref = '$0::377894440:$1:203060187#+0..1'

segment_ref_list = segment_ref.split(':')
partition_name = segment_ref_list[2]
segment_id_ref = segment_ref_list[4]
segment_id = segment_id_ref.split('#')[0]
segment_direction = segment_id_ref.split('#')[1][0]
segment_offset = segment_id_ref.split('#')[1][1:]
segment_offset_start = segment_offset.split('..')[0]
segment_offset_end = segment_offset.split('..')[1]

segment_ref_partition_center = HereQuad.from_long_key(int(partition_name)).bounding_box.center
segment_ref_partition_center_lat = segment_ref_partition_center.lat
segment_ref_partition_center_lng = segment_ref_partition_center.lng
hmc_partition_level_12 = HereQuad.from_lat_lng_level(segment_ref_partition_center_lat, segment_ref_partition_center_lng,
                                                     12)

pvid = hmc_external_reference.segment_to_pvid(partition_id=hmc_partition_level_12.long_key,
                                              segment_ref=Ref(partition=Partition(str(hmc_partition_level_12.long_key)),
                                                              identifier=Identifier(
                                                                  'here:cm:segment:{}'.format(segment_id))))

print('pvid', pvid)
