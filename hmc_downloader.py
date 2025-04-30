"""
This code defines a class HmcDownloader that is responsible for downloading
and processing data from the HERE platform. It provides methods to retrieve
schema, download data, and extract country tile and admin indexes.
"""

import json
import os
import subprocess
import threading
from typing import Optional, List

from google.protobuf.json_format import MessageToJson
from here.platform.adapter import DecodedMessage
from here.platform.catalog import Catalog
from here.platform.partition import Partition

from hmc_download_options import FileFormat
from concurrent.futures import ThreadPoolExecutor, as_completed


def safe_run_cli_command(cmd: str):
    cmd = ' '.join(cmd.strip().split())
    try:
        subprocess.run(cmd, check=True, shell=True)
    except subprocess.CalledProcessError as e:
        raise


def detect_partition_version(catalog, layer_name, partition_id):
    list_cmd = [
        "olp", "catalog", "layer", "partition", "list",
        catalog.hrn, layer_name,
        "--filter", str(partition_id),
        "--json"
    ]
    print(" ".join(list_cmd))
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
    ver = int(data_handle.split('.')[-1])
    return ver


def download_partition(method, catalog, layer_name, partition_id, taget_output_filepath, version=None,
                       file_format="json"):
    # os.makedirs(output_file_path, exist_ok=True)
    if method == 'sdk':
        layer = catalog.get_layer(layer_name)
        partitions = layer.read_partitions((partition_id,), version)
        _, partition_content = partitions[0]
        json_obj = MessageToJson(DecodedMessage(partition_content))
        output_path = os.path.join(taget_output_filepath, f"{partition_id}.json")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(json.loads(json_obj), f, indent=2)
        print(f"Download completed（SDK）：{output_path}")
    elif method == 'cli':
        output_folder = os.path.dirname(taget_output_filepath)
        os.makedirs(output_folder, exist_ok=True)

        cmd = (
            f'olp catalog layer partition get '
            f'{catalog.hrn} {layer_name} '
            f'--partitions {partition_id} '
            f'--decode true '
            f'> "{taget_output_filepath}"'
        )
        if os.path.exists(taget_output_filepath):
            os.remove(taget_output_filepath)
        process = subprocess.Popen(cmd, shell=True)
        return_code = process.wait()
        if return_code != 0:
            raise RuntimeError(f"CLI 指令失敗，返回碼 {return_code}")

        print(f"Download Completed（CLI）：{taget_output_filepath}")
    else:
        raise ValueError(f"Unknown download method：{method}")


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
            max_workers: int = 5,  # 新增參數：一次同時跑幾個
    ):
        self.set_tiling_scheme(tiling_scheme)

        def download_one(partition_id):
            try:
                if version is None:
                    ver = detect_partition_version(self.catalog, self.layer, partition_id)
                else:
                    ver = version

                output_path = self.get_cli_output_filepath(partition_id, ver)

                download_partition(
                    method="cli",
                    catalog=self.catalog,
                    layer_name=self.layer,
                    partition_id=partition_id,
                    taget_output_filepath=output_path,
                    version=ver
                )
            except Exception as e:
                print(f"❌ Partition {partition_id} failed：{e}")


        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for pid in quad_ids:
                futures.append(executor.submit(download_one, pid))

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"❌ Task failure：{e}")

        for partition_id in quad_ids:
            if version is None:
                ver = detect_partition_version(self.catalog, self.layer, partition_id)
            else:
                ver = version

            output_file_path = self.get_cli_output_filepath(partition_id, ver)

            download_partition(
                method="cli",
                catalog=self.catalog,
                layer_name=self.layer,
                partition_id=partition_id,
                taget_output_filepath=output_file_path,
                version=ver
            )

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

    def get_cli_output_filepath(self, partition_id: int, version: Optional[int]) -> str:
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
