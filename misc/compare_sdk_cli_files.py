import os
import json
import pandas as pd


def flatten_dict(obj, parent_key=''):
    """
    Recursively flattens a JSON object into a dict of key paths to values.
    Lists are flattened by iterating elements under the same parent key.
    """
    items = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f"{parent_key}.{k}" if parent_key else k
            if isinstance(v, (dict, list)):
                items.update(flatten_dict(v, new_key))
            else:
                items[new_key] = v
    elif isinstance(obj, list):
        for v in obj:
            items.update(flatten_dict(v, parent_key))
    return items


def compare_sdk_cli_files_with_values_to_csv(directory: str, output_dir: str):
    """
    Compare JSON files (SDK vs CLI) including nested keys and values,
    and write flattened key-value diffs and summary to CSV.
    """
    os.makedirs(output_dir, exist_ok=True)

    files = os.listdir(directory)
    sdk_files = [f for f in files if f.endswith('.json') and not f.endswith('_olpcli.json')]

    diff_records = []
    sdk_missing_counts = {}
    cli_missing_counts = {}

    for sdk_file in sdk_files:
        base = sdk_file[:-5]
        cli_file = f"{base}_olpcli.json"
        sdk_path = os.path.join(directory, sdk_file)
        cli_path = os.path.join(directory, cli_file)
        if not os.path.exists(cli_path):
            continue

        with open(sdk_path, 'r', encoding='utf-8') as f:
            sdk_data = json.load(f)
        with open(cli_path, 'r', encoding='utf-8') as f:
            cli_data = json.load(f)

        # Flatten to key->value maps
        sdk_flat = flatten_dict(sdk_data)
        cli_flat = flatten_dict(cli_data)

        sdk_keys = set(sdk_flat.keys())
        cli_keys = set(cli_flat.keys())
        sdk_only = sdk_keys - cli_keys
        cli_only = cli_keys - sdk_keys

        # Build key=value strings
        sdk_only_kv = '; '.join(f"{k}={sdk_flat[k]!r}" for k in sorted(sdk_only))
        cli_only_kv = '; '.join(f"{k}={cli_flat[k]!r}" for k in sorted(cli_only))

        diff_records.append({
            'file_base': base,
            'sdk_only_kv': sdk_only_kv,
            'cli_only_kv': cli_only_kv,
            'sdk_only_count': len(sdk_only),
            'cli_only_count': len(cli_only)
        })

        for key in sdk_only:
            cli_missing_counts[key] = cli_missing_counts.get(key, 0) + 1
        for key in cli_only:
            sdk_missing_counts[key] = sdk_missing_counts.get(key, 0) + 1

    # Create DataFrames
    df_diff = pd.DataFrame(diff_records)
    all_keys = set(sdk_missing_counts) | set(cli_missing_counts)
    summary_rows = [{
        'key_path': key,
        'missing_in_sdk': sdk_missing_counts.get(key, 0),
        'missing_in_cli': cli_missing_counts.get(key, 0)
    } for key in sorted(all_keys)]
    df_summary = pd.DataFrame(summary_rows)

    # Write to excel
    diff_excel = os.path.join(output_dir, "per_file_key_value_differences.xlsx")
    summary_excel = os.path.join(output_dir, "key_value_summary.xlsx")
    df_diff.to_excel(diff_excel, index=False)
    df_summary.to_excel(summary_excel, index=False)

    print(f"Key-value diffs written to: {diff_excel}")
    print(f"Summary written to: {summary_excel}")


# Run the comparison
compare_sdk_cli_files_with_values_to_csv("decoded/hrn_here_data__olp-here_rib-2/heretile/20803994", "decoded/hrn_here_data__olp-here_rib-2/heretile/20803994/output")

