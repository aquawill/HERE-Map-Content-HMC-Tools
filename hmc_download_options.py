from enum import Enum


class FileFormat(Enum):
    JSON = 1
    TXTBP = 2


class QueryType(Enum):
    LAT_LNG = 1  # Latitude and Longitude
    QUAD_ID = 2  # ID of RIB2 Partition or HDLM Tile
    BOUNDING_BOX = 3


class HerePlatformCatalog(Enum):
    HDLM_WEU_2 = 1
    HMC_RIB_2 = 2
    HMC_EXT_REF_2 = 3
    CLASSIC_HERE_LANES_WEU = 4
    ONEMAP_HERE_LANES_WEU = 5

class DownloadMethod(Enum):
    DATA_SDK = 1
    OLP_CLI = 2
