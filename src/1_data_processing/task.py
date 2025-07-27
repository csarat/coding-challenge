import csv
import json
import sqlite3
import os
import zipfile
import io
import requests
from datetime import datetime
from statistics import mean

# Load data from various sources
def load_data(source_type: str, path_or_url: str):
    data = []
    # plain CSV file
    if source_type == "csv":
        with open(path_or_url, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(row)
    # JSON file
    elif source_type == "json":
        with open(path_or_url, "r") as f:
            data = json.load(f)
    # SQLite DB table named "raw_data"
    elif source_type == "sqlite":
        conn = sqlite3.connect(path_or_url)
        cur = conn.cursor()
        cur.execute("SELECT * FROM raw_data")
        rows = cur.fetchall()
        columns = [c[0] for c in cur.description]
        for row in rows:
            data.append(dict(zip(columns, row)))
        conn.close()
    # zipped archive containing at least one CSV file
    elif source_type == "zip_csv":
        with zipfile.ZipFile(path_or_url, "r") as z:
            for fname in z.namelist():
                if fname.endswith(".csv"):
                    with z.open(fname) as f:
                        reader = csv.DictReader(io.TextIOWrapper(f))
                        for row in reader:
                            data.append(row)
    # remote API returning JSON array
    elif source_type == "api":
        response = requests.get(path_or_url)
        if response.status_code == 200:
            data = response.json()
        else:
            raise Exception(f"API call failed with status {response.status_code}")
    else:
        raise Exception("Unsupported source type: %s" % source_type)
    return data

# Clean data using various strategies
def clean_data(data: list, strategy: str):
    cleaned = []
    if strategy == "drop_nulls":
        for record in data:
            if all(v not in (None, "", "null") for v in record.values()):
                cleaned.append(record)
    elif strategy == "fill_nulls":
        for record in data:
            new_rec = {}
            for k, v in record.items():
                new_rec[k] = v if v not in (None, "", "null") else "N/A"
            cleaned.append(new_rec)
    elif strategy == "cast_types":
        for record in data:
            new_rec = {}
            for k, v in record.items():
                if isinstance(v, str) and v.isdigit():
                    new_rec[k] = int(v)
                else:
                    try:
                        new_rec[k] = float(v)
                    except (ValueError, TypeError):
                        new_rec[k] = v
            cleaned.append(new_rec)
    elif strategy == "standardize_case":
        for record in data:
            new_rec = {}
            for k, v in record.items():
                if isinstance(k, str):
                    new_key = k.strip().lower().replace(" ", "_")
                else:
                    new_key = k
                if isinstance(v, str):
                    new_val = v.strip().lower()
                else:
                    new_val = v
                new_rec[new_key] = new_val
            cleaned.append(new_rec)
    elif strategy == "remove_duplicates":
        seen = set()
        for record in data:
            key = tuple(sorted(record.items()))
            if key not in seen:
                seen.add(key)
                cleaned.append(record)
    else:
        raise Exception("Unknown cleaning strategy: %s" % strategy)
    return cleaned

# Transform data in simple ways
def transform_data(data: list, transformation: str):
    transformed = []
    if transformation == "filter_future_dates":
        for record in data:
            timestamp = record.get("timestamp")
            try:
                ts = datetime.strptime(timestamp, "%Y-%m-%d")
                if ts <= datetime.now():
                    transformed.append(record)
            except Exception:
                transformed.append(record)
    elif transformation == "flag_outliers":
        # mark numeric "value" fields that are >2 standard deviations from mean
        values = [float(r["value"]) for r in data if "value" in r and r["value"] not in (None, "", "null")]
        if values:
            avg = mean(values)
            sqdiff = [(x - avg) ** 2 for x in values]
            std = (sum(sqdiff) / len(values)) ** 0.5
            for record in data:
                rec = dict(record)
                try:
                    val = float(record.get("value", 0))
                    rec["is_outlier"] = val > (avg + 2 * std)
                except Exception:
                    rec["is_outlier"] = False
                transformed.append(rec)
    elif transformation == "aggregate_by_category":
        # produce aggregated counts by a "category" key
        agg = {}
        for record in data:
            cat = record.get("category", "unknown")
            agg.setdefault(cat, 0)
            agg[cat] += 1
        # represent the aggregate as a list of dicts
        for k, v in agg.items():
            transformed.append({"category": k, "count": v})
    else:
        # default: no transformation
        transformed = data
    return transformed

# Persist or output the transformed data
def output_results(data: list, output_format: str, output_path: str = None):
    if output_format == "csv":
        if output_path and data:
            with open(output_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
    elif output_format == "json":
        if output_path:
            with open(output_path, "w") as f:
                json.dump(data, f, indent=4)
    elif output_format == "sqlite":
        if output_path and data:
            conn = sqlite3.connect(output_path)
            cur = conn.cursor()
            cols = ", ".join([f"{c} TEXT" for c in data[0].keys()])
            cur.execute(f"CREATE TABLE IF NOT EXISTS results ({cols})")
            for record in data:
                placeholders = ", ".join("?" for _ in record)
                cur.execute(
                    f"INSERT INTO results VALUES ({placeholders})",
                    tuple(str(v) for v in record.values())
                )
            conn.commit()
            conn.close()
    elif output_format == "print":
        # fall back to printing results to console
        for row in data:
            print(row)
    else:
        raise Exception("Unsupported output format: %s" % output_format)

def main():
    source_type = "csv"
    source_path = "/Users/csarat/git/projects/coding-challenge/resources/input/test.csv"
    cleaning_strategy = "standardize_case"
    transformation = "aggregate_by_category"
    output_format = os.environ.get("OUTPUT_FORMAT", "print")
    output_path = "/Users/csarat/git/projects/coding-challenge/resources/output/"

    data = load_data(source_type, source_path)
    cleaned = clean_data(data, cleaning_strategy)
    transformed = transform_data(cleaned, transformation)
    output_results(cleaned, output_format, output_path)

if __name__ == "__main__":
    main()
