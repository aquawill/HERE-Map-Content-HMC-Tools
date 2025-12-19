import os

<<<<<<<< HEAD:converter_run.py
<<<<<<<< HEAD:converter_run.py
rib2_partition_path = r'decoded/hrn_here_data__olp-here_rib-2/heretile'
========
rib2_partition_path = r'../decoded/hrn_here_data__olp-here_rib-2/heretile'
>>>>>>>> d563863 (1. Restructure converters.):hmc_converter/converter_run.py
========
rib2_partition_path = r'../decoded/hrn_here_data__olp-here_rib-2/heretile'
>>>>>>>> refs/remotes/origin/refactor-20250429:hmc_converter/converter_run.py
ext_ref2_partition_path = r'../decoded/hrn_here_data__olp-here_rib-external-references-2/'
overwrite = 'y'

for r, ds, fs in os.walk(rib2_partition_path):
    if len(ds) > 0:
        for d in ds:
            partition_folder = os.path.join(r, d)

            os.system('echo running "python hmc_topology_to_geojson.py {} {}"'.format(partition_folder, overwrite))
            os.system('python hmc_topology_to_geojson.py {} {}'.format(partition_folder, overwrite))
            
            os.system('echo running "python hmc_road_based_attributes_to_geojson.py {}"'.format(partition_folder))
            os.system('python hmc_road_based_attributes_to_geojson.py {} {}'.format(partition_folder, overwrite))

            os.system('echo running "python hmc_address_locations_to_geojson.py {}"'.format(partition_folder))
            os.system('python hmc_address_locations_to_geojson.py {} {}'.format(partition_folder, overwrite))

            os.system('echo running "python hmc_polygon_to_geojson.py {}"'.format(partition_folder))
            os.system('python hmc_polygon_to_geojson.py {} {}'.format(partition_folder, overwrite))

            os.system('echo running "python hmc_places_to_geojson.py {}"'.format(partition_folder))
            os.system('python hmc_places_to_geojson.py {} {}'.format(partition_folder, overwrite))

            os.system('echo running "python hmc_enhanced_buildings_to_geojson.py {}"'.format(partition_folder))
            os.system('python hmc_enhanced_buildings_to_geojson.py {} {}'.format(partition_folder, overwrite))

            os.system('echo running "python hmc_postal_code_points_to_geojson.py {}"'.format(partition_folder))
            os.system('python hmc_postal_code_points_to_geojson.py {} {}'.format(partition_folder, overwrite))

            os.system('echo running "python hmc_landmarks_to_geojson.py {}"'.format(partition_folder))
            os.system('python hmc_landmarks_to_geojson.py {} {}'.format(partition_folder, overwrite))

            os.system('echo running "python hmc_parking_areas_to_geojson.py {}"'.format(partition_folder))
            os.system('python hmc_parking_areas_to_geojson.py {} {}'.format(partition_folder, overwrite))

            os.system('echo running "python hmc_postal_code_points_to_geojson.py {}"'.format(partition_folder))
            os.system('python hmc_postal_code_points_to_geojson.py {} {}'.format(partition_folder, overwrite))

            os.system('echo running "python hmc_lane_attributes_to_geojson.py {}"'.format(partition_folder))
            os.system('python hmc_lane_attributes_to_geojson.py {} {}'.format(partition_folder, overwrite))

            os.system('echo running "python hmc_evcp_v2_to_geojson.py {}"'.format(partition_folder))
            os.system('python hmc_evcp_v2_to_geojson.py {} {}'.format(partition_folder, overwrite))

for r, ds, fs in os.walk(ext_ref2_partition_path):
    if len(ds) > 0:
        for d in ds:
            ext_ref_partition_folder = os.path.join(r, d)
            os.system('echo running "python hmc_external_reference_attributes_to_geojson.py {}"'.format(
                ext_ref_partition_folder))
            os.system('python hmc_external_reference_attributes_to_geojson.py {} {}'.format(ext_ref_partition_folder, overwrite))
