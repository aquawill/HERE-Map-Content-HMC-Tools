"""
This code defines a class HmcDownloader that is responsible for downloading
and processing data from the HERE platform. It provides methods to retrieve
schema, download data, and extract country tile and admin indexes.
"""

import json
import os
import subprocess
from typing import Optional, List

from google.protobuf.json_format import MessageToJson
from here.platform.adapter import DecodedMessage
from here.platform.catalog import Catalog
from here.platform.partition import Partition

from hmc_download_options import FileFormat


class HmcDownloader:
    """
    The HmcDownloader class is responsible for downloading and processing data from the HERE platform. It provides methods to retrieve schema, download data, and extract country tile and admin indexes.

    The class has the following attributes:
    - `catalog`: A Catalog object that provides access to the HERE platform data.
    - `layer`: The name of the layer to be downloaded.
    - `version`: The version of partition should be downloaded.
    - `quad_ids`: A list of quad IDs to be downloaded.
    - `file_format`: The file format to be used for the downloaded data.
    - `tiling_scheme`: The tiling scheme to be used for the downloaded data.
    - `output_file_path`: The path to the output file.

    The class provides the following methods:
    - `get_output_file_path()`: Returns the path to the output file.
    - `set_tiling_scheme(tiling_scheme: str)`: Sets the tiling scheme to be used for the downloaded data.
    - `get_schema()`: Retrieves the schema for the specified layer.
    - `partition_file_writer(partition: Partition)`: Writes the downloaded data to a file.
    - `download_generic_layer()`: Downloads the data for the specified layer using the generic tiling scheme.
    - `download_generic_layer(quad_ids: list)`: Downloads the data for the specified layer and quad IDs using the generic tiling scheme.
    - `download_partitioned_layer(quad_ids: list)`: Downloads the data for the specified layer and quad IDs using the partitioned tiling scheme.
    - `get_country_tile_indexes(iso_country_code_tuple: tuple)`: Retrieves the country tile indexes for the specified ISO country codes.
    - `get_country_admin_indexes(iso_country_code_tuple: tuple)`: Retrieves the country admin indexes for the specified ISO country codes.
    """

    catalog: Catalog
    layer: str = ""
    version: int
    quad_ids: list
    file_format: FileFormat
    tiling_scheme: str
    output_file_path: str
    partition_data: None

    def __init__(self, catalog: Catalog, layer: str, file_format: FileFormat, version: int = None) -> None:
        super().__init__()  # Initialize the class with the provided catalog, layer, and file format
        self.catalog = catalog
        self.layer = layer
        self.file_format = file_format
        self.version = version

    def get_output_file_path(self):
        return self.output_file_path

    def set_tiling_scheme(self, tiling_scheme: str):
        self.tiling_scheme = tiling_scheme
        return self

    def get_partition_data(self):
        return self.partition_data

    def get_schema(self):
        return self.catalog.get_layer(
            self.layer
        ).get_schema()  # Retrieve the schema for the specified layer

    def partition_file_writer(self, partition: Partition):
        versioned_partition, partition_content = (
            partition  # Unpack the versioned partition and partition content
        )
        hrn_folder_name = self.catalog.hrn.replace(
            ":", "_"
        )  # Replace ':' with '_' in the catalog HRN
        extension: str
        if (
                self.file_format == FileFormat.TXTBP
        ):  # Check the file format and set the extension accordingly
            extension = "txtbp"
        elif self.file_format == FileFormat.JSON:
            extension = "json"
        filename = os.path.join(
            "decoded",
            hrn_folder_name,
            self.tiling_scheme,
            str(versioned_partition.id),
            # Construct the filename
            "{}_{}_v{}.{}".format(
                self.layer,
                versioned_partition.id,
                versioned_partition.version,
                extension,
            ),
        )
        if not os.path.exists(filename):  # Check if the file already exists
            if not os.path.exists(
                    "decoded"
            ):  # Create 'decoded' directory if it doesn't exist
                os.mkdir("decoded")
            if not os.path.exists(
                    os.path.join("decoded", hrn_folder_name)
            ):  # Create HRN directory if it doesn't exist
                os.mkdir(os.path.join("decoded", hrn_folder_name))
            if not os.path.exists(
                    os.path.join("decoded", hrn_folder_name, self.tiling_scheme)
            ):  # Create HRN directory if it doesn't exist
                os.mkdir(os.path.join("decoded", hrn_folder_name, self.tiling_scheme))
            if not os.path.exists(
                    os.path.join(
                        "decoded",
                        hrn_folder_name,
                        self.tiling_scheme,
                        str(versioned_partition.id),
                    )
            ):  # Create partition directory if it doesn't exist
                os.mkdir(
                    os.path.join(
                        "decoded",
                        hrn_folder_name,
                        self.tiling_scheme,
                        str(versioned_partition.id),
                    )
                )
            print(
                "layer: {} | partition: {} | version: {} | size: {} bytes".format(
                    self.layer,
                    # Print information about the layer, partition, version, and size
                    versioned_partition.id,
                    versioned_partition.version,
                    versioned_partition.data_size,
                )
            )
            decoded_content = DecodedMessage(
                partition_content
            )  # Decode the partition content
            with open(
                    filename, mode="w", encoding="utf-8"
            ) as output:  # Open the file for writing
                content_to_write: str
                if (
                        self.file_format == FileFormat.TXTBP
                ):  # Check the file format and set the content to write accordingly
                    content_to_write = str(decoded_content)
                elif self.file_format == FileFormat.JSON:
                    content_to_write = MessageToJson(decoded_content)
                output.write(content_to_write)  # Write the content to the file
                print({"filename": filename, "result": "created"})
                self.output_file_path = filename
        else:
            print({"filename": filename, "result": "skipped"})
            self.output_file_path = filename

    def download_generic_layer(self):
        self.set_tiling_scheme("generic")
        generic_layer = self.catalog.get_layer(
            self.layer
        )  # Get the versioned layer for the specified layer
        if generic_layer.get_schema():
            partitions = generic_layer.read_partitions()
            for p in partitions:
                self.partition_file_writer(p)

    def olp_cli_download_partition(
            self,
            tiling_scheme: str,
            quad_ids: List[int],
            write_to_file: bool = True,
            version: Optional[int] = None,
    ):
        """
        For each partition ID in quad_ids:
          1. If version is None, call 'olp catalog layer partition list ... --json'
             and parse out the version from dataHandle.
          2. Build the 'olp catalog layer partition get' command with that version.
          3. Redirect its output into the path returned by get_output_filepath().
        """
        self.set_tiling_scheme(tiling_scheme)
        for partition_id in quad_ids:
            # 1. Determine which version to fetch
            if version is None:
                list_cmd = [
                    'olp', 'catalog', 'layer', 'partition', 'list',
                    self.catalog.hrn, self.layer,
                    '--filter', str(partition_id),
                    '--json'
                ]
                # run the list command and parse JSON
                res = subprocess.run(
                    list_cmd,
                    capture_output=True,
                    shell=True,
                    check=True
                )
                payload = json.loads(res.stdout)
                items = payload.get('results', {}).get('items', [])
                if not items:
                    raise RuntimeError(f"No partition info found for {partition_id}")
                data_handle = items[0]['dataHandle']
                # extract version suffix after the last '.'
                ver = int(data_handle.split('.')[-1])
            else:
                ver = version

            # 2. Build the get command
            get_cmd = [
                'olp', 'catalog', 'layer', 'partition', 'get',
                self.catalog.hrn, self.layer,
                '--partitions', str(partition_id),
                '--version', str(ver),
                '--decode', 'true'
            ]

            print(' '.join(get_cmd))

            if write_to_file:
                # 3a. Decide output path
                output_file = self.get_output_filepath(partition_id, ver)
                # 3b. Add shell redirection
                full_cmd = ' '.join(get_cmd) + f' > "{output_file}"'
                # 3c. Execute
                result = subprocess.run(
                    full_cmd,
                    shell=True,
                    check=True
                )
                print(f"Written: {output_file} (return code: {result.returncode})")
                self.output_file_path = output_file

    def download_generic_layer(self, quad_ids: list, version: int):
        self.set_tiling_scheme("generic")
        versioned_layer = self.catalog.get_layer(
            self.layer
        )  # Get the versioned layer for the specified layer
        partitions = versioned_layer.read_partitions(
            quad_ids, version
        )  # Read partitions for the specified quad IDs
        for p in partitions:
            self.partition_file_writer(p)

    def get_output_filepath(self, partition_id: int, version: Optional[int]) -> str:
        """
        Determine the output path for a given partition and version,
        create directories if necessary, and return the full file path.
        """
        hrn_folder = self.catalog.hrn.replace(':', '_')
        base_dir = os.path.join(
            'decoded',
            hrn_folder,
            self.tiling_scheme,
            str(partition_id)
        )
        os.makedirs(base_dir, exist_ok=True)

        filename = f"{self.layer}_{partition_id}"
        if version is not None:
            filename += f"_v{version}_olpcli"
        filename += ".json"

        return os.path.join(base_dir, filename)

    def download_partitioned_layer(self, quad_ids: list, write_to_file: bool = True, version: int = None):
        self.set_tiling_scheme("heretile")
        versioned_layer = self.catalog.get_layer(
            self.layer
        )  # Get the versioned layer for the specified layer
        partitions = versioned_layer.read_partitions(
            quad_ids, version
        )  # Read partitions for the specified quad IDs
        if write_to_file:
            for p in partitions:
                self.partition_file_writer(p)
        else:
            self.partition_data = partitions
        return self

    def get_country_tile_indexes(self, iso_country_code_tuple: tuple):
        layer = self.catalog.get_layer(
            self.layer
        )  # Get the layer for the specified layer
        partitions = layer.read_partitions(
            iso_country_code_tuple
        )  # Read partitions for the specified ISO country code tuple
        results = []
        for p in partitions:
            versioned_partition, partition_content = (
                p  # Unpack the versioned partition and partition content
            )
            decoded_content_json = json.loads(
                MessageToJson(DecodedMessage(partition_content))
            )  # Decode the partition content to JSON
            results.append(
                {decoded_content_json["partitionName"]: decoded_content_json["tileId"]}
            )  # Append partition name and tile ID to results
        return results  # Return the results

    def get_country_admin_indexes(self, iso_country_code_tuple: tuple):
        layer = self.catalog.get_layer(
            self.layer
        )  # Get the layer for the specified layer
        partitions = layer.read_partitions(
            iso_country_code_tuple
        )  # Read partitions for the specified ISO country code tuple
        results = []
        for p in partitions:
            versioned_partition, partition_content = (
                p  # Unpack the versioned partition and partition content
            )
            hmc_json = json.loads(
                MessageToJson(DecodedMessage(partition_content))
            )  # Decode the partition content to JSON
            tile_id_list = hmc_json["tileId"]  # Get the tile ID list from the JSON
            indexed_location_list = hmc_json[
                "indexedLocation"
            ]  # Get the indexed location list from the JSON
            for indexed_location in indexed_location_list:
                indexed_location_tile_index_list = indexed_location[
                    "tileIndex"
                ]  # Get the tile index list from the indexed location
                indexed_location_boundary_tile_index_list = indexed_location[
                    "boundaryTileIndex"
                ]  # Get the boundary tile index list from the indexed location
                del indexed_location[
                    "tileIndex"
                ]  # Delete the tile index from the indexed location
                del indexed_location[
                    "boundaryTileIndex"
                ]  # Delete the boundary tile index from the indexed location
                indexed_location["partitionIdList"] = (
                    []
                )  # Initialize the partition ID list in the indexed location
                indexed_location["boundaryPartitionIdList"] = (
                    []
                )  # Initialize the boundary partition ID list in the indexed location
                for indexed_location_tile_index in indexed_location_tile_index_list:
                    indexed_location["partitionIdList"].append(
                        tile_id_list[indexed_location_tile_index]
                    )  # Append partition ID to the partition ID list
                for (
                        indexed_location_boundary_tile_index
                ) in indexed_location_boundary_tile_index_list:
                    indexed_location["boundaryPartitionIdList"].append(
                        # Append boundary partition ID to the boundary partition ID list
                        tile_id_list[indexed_location_boundary_tile_index]
                    )
            hmc_json["indexedLocation"] = (
                indexed_location_list  # Update the indexed location list in the HMC JSON
            )
            print(json.dumps(hmc_json, indent="    "))  # Print the formatted HMC JSON
        return results  # Return the results
