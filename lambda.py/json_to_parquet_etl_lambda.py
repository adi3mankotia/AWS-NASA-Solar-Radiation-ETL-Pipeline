import json
import boto3
import pandas as pd
import io
from urllib.parse import unquote_plus
from datetime import datetime

s3 = boto3.client("s3")

OUTPUT_BUCKET = "adi-elt-project"
OUTPUT_PREFIX = "nasa_parquet_datalake/"

PARAMETER_MAP = {
    "T2M": "temperature_c",
    "RH2M": "relative_humidity_percent",
    "WS10M": "wind_speed_10m_m_s",
    "PRECTOTCORR": "precipitation_mm_day",
    "ALLSKY_SFC_SW_DWN": "solar_radiation_kwh_m2_day"
}

FILL_VALUE = -999.0


def clean_value(value):
    if value == FILL_VALUE:
        return None
    return value


def flatten_nasa_json(data, source_key):
    parameter_data = data["properties"]["parameter"]

    city_metadata = data.get("city_metadata", {})

    city = city_metadata.get("city", "unknown")
    latitude = city_metadata.get("latitude", data["geometry"]["coordinates"][1])
    longitude = city_metadata.get("longitude", data["geometry"]["coordinates"][0])

    dates = parameter_data["T2M"].keys()

    rows = []

    for date_key in dates:
        date_obj = datetime.strptime(date_key, "%Y%m%d")

        row = {
            "date": date_obj.strftime("%Y-%m-%d"),
            "date_key": date_key,
            "year": date_obj.year,
            "month": date_obj.month,
            "day": date_obj.day,
            "city": city,
            "latitude": latitude,
            "longitude": longitude,
            "source_file": source_key
        }

        for nasa_param, clean_col in PARAMETER_MAP.items():
            row[clean_col] = clean_value(
                parameter_data.get(nasa_param, {}).get(date_key)
            )

        solar_value = row.get("solar_radiation_kwh_m2_day")

        if solar_value is None:
            row["solar_potential_category"] = None
        elif solar_value >= 5:
            row["solar_potential_category"] = "high"
        elif solar_value >= 3:
            row["solar_potential_category"] = "medium"
        else:
            row["solar_potential_category"] = "low"

        rows.append(row)

    return pd.DataFrame(rows)


def lambda_handler(event, context):
    processed_files = []

    for record in event["Records"]:
        input_bucket = record["s3"]["bucket"]["name"]
        input_key = unquote_plus(record["s3"]["object"]["key"])

        print(f"Reading file from s3://{input_bucket}/{input_key}")

        response = s3.get_object(
            Bucket=input_bucket,
            Key=input_key
        )

        raw_content = response["Body"].read().decode("utf-8")
        data = json.loads(raw_content)

        df = flatten_nasa_json(data, input_key)

        print("Flattened dataframe preview:")
        print(df.head())
        print(f"Rows created: {len(df)}")

        if df.empty:
            print(f"No rows found in {input_key}")
            continue

        # Use these values for the S3 partition path
        city = df["city"].iloc[0]
        year = int(df["year"].iloc[0])
        month = int(df["month"].iloc[0])
        date_key = df["date_key"].iloc[0]

        # IMPORTANT:
        # Drop partition columns from the Parquet file itself.
        # Glue/Athena will read city/year/month from the S3 folder path:
        # city=toronto/year=2026/month=06/
        df_to_write = df.drop(
            columns=["city", "year", "month"],
            errors="ignore"
        )

        parquet_buffer = io.BytesIO()

        df_to_write.to_parquet(
            parquet_buffer,
            engine="pyarrow",
            index=False
        )

        output_key = (
            f"{OUTPUT_PREFIX}"
            f"city={city}/"
            f"year={year}/"
            f"month={month:02d}/"
            f"nasa_power_{city}_{date_key}.parquet"
        )

        s3.put_object(
            Bucket=OUTPUT_BUCKET,
            Key=output_key,
            Body=parquet_buffer.getvalue()
        )

        print(f"Wrote Parquet file to s3://{OUTPUT_BUCKET}/{output_key}")

        processed_files.append(output_key)

    return {
        "statusCode": 200,
        "message": "NASA JSON transformed to Parquet successfully",
        "processed_files": processed_files
    }
