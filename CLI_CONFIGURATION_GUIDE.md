# `cli_config.yaml` Sample

```yaml
catalog: HMC_RIB_2           # Choose the target Catalog: HMC_RIB_2, HDLM_WEU_2, or HMC_EXT_REF_2
version: null                # Optional; set to null to use the latest version
download_method: OLP_CLI     # Download method: DATA_SDK or OLP_CLI

target:
  type: bounding_box         # Supported types: coordinate, bounding_box, tile_id_list, country
  bounding_box:
    west: 97.735
    south: 9.591
    east: 106.087
    north: 20.981

layers:
  - adas-attributes
  - topology-geometry
  - navigation-attributes
  - advanced-navigation-attributes
```

---

### YAML Configuration Guide (`CONFIGURATION_GUIDE.md`)

#### Required Fields

| Field             | Description                                                                                    |
| ----------------- | ---------------------------------------------------------------------------------------------- |
| `catalog`         | The target HERE Platform catalog. Supported values: `HMC_RIB_2`, `HDLM_WEU_2`, `HMC_EXT_REF_2` |
| `version`         | (Optional) Specific version to download. Use `null` to fetch the latest version                |
| `download_method` | Choose the download method: `DATA_SDK` (via SDK API) or `OLP_CLI` (via OLP CLI)                |
| `target`          | Defines the geographic query method. See the subfields below                                   |
| `layers`          | List of layer IDs to download (must be strings)                                                |

#### `target` Format Options

| type           | Subfields & Description                                                              |
| -------------- | ------------------------------------------------------------------------------------ |
| `coordinate`   | `coordinate: { lat: <latitude>, lng: <longitude> }`                                  |
| `bounding_box` | `bounding_box: { west: <minLng>, south: <minLat>, east: <maxLng>, north: <maxLat> }` |
| `tile_id_list` | `tile_ids: [<tileId1>, <tileId2>, ...]`                                              |
| `country`      | `countries: ["TWN", "PHL"]` — list of ISO 3166-1 alpha-3 country codes               |

---

### How to Run

```bash
python cli_downloader.py --config cli_config.yaml
```