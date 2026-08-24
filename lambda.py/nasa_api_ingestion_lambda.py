import json
import boto3
import urllib.request
from datetime import datetime, timedelta

s3 = boto3.client("s3")

BUCKET_NAME = "adi-elt-project"
RAW_PREFIX = "nasa_json_incoming/"

PARAMETERS_LIST = [
    "T2M",
    "RH2M",
    "WS10M",
    "PRECTOTCORR",
    "ALLSKY_SFC_SW_DWN"
]

PARAMETERS = ",".join(PARAMETERS_LIST)
FILL_VALUE = -999.0

CITIES = [
    {
        "city": "toronto",
        "latitude": 43.6532,
        "longitude": -79.3832
    },
    {
        "city": "saskatoon",
        "latitude": 52.1579,
        "longitude": -106.6702
    },
    {
        "city": "calgary",
        "latitude": 51.0447,
        "longitude": -114.0719
    }
]


def call_nasa_api(latitude, longitude, date_str):
    url = (
        "https://power.larc.nasa.gov/api/temporal/daily/point"
        f"?parameters={PARAMETERS}"
        "&community=RE"
        f"&longitude={longitude}"
        f"&latitude={latitude}"
        f"&start={date_str}"
        f"&end={date_str}"
        "&format=JSON"
    )

    print(f"Calling NASA API: {url}")

    with urllib.request.urlopen(url) as response:
        return json.loads(response.read().decode("utf-8"))


def has_valid_values(data, date_str):
    parameter_data = data.get("properties", {}).get("parameter", {})

    for param in PARAMETERS_LIST:
        value = parameter_data.get(param, {}).get(date_str)

        if value is None:
            print(f"{param} is missing for {date_str}")
            return False

        if value == FILL_VALUE:
            print(f"{param} is fill value -999.0 for {date_str}")
            return False

    return True


def get_latest_available_data(latitude, longitude, max_days_back=21):
    """
    Tries yesterday, then 2 days ago, then 3 days ago, etc.
    Stops when NASA returns real values instead of -999.0.
    """

    for days_back in range(1, max_days_back + 1):
        target_date = datetime.utcnow().date() - timedelta(days=days_back)
        date_str = target_date.strftime("%Y%m%d")

        data = call_nasa_api(latitude, longitude, date_str)

        if has_valid_values(data, date_str):
            print(f"Valid NASA data found for {date_str}")
            return target_date, date_str, data

        print(f"No valid NASA data for {date_str}. Trying older date...")

    raise ValueError(
        f"No valid NASA POWER data found in the last {max_days_back} days."
    )


def lambda_handler(event, context):
    uploaded_files = []

    for city in CITIES:
        city_name = city["city"]
        latitude = city["latitude"]
        longitude = city["longitude"]

        target_date, date_str, data = get_latest_available_data(
            latitude=latitude,
            longitude=longitude,
            max_days_back=21
        )

        data["city_metadata"] = {
            "city": city_name,
            "latitude": latitude,
            "longitude": longitude,
            "data_date": date_str,
            "date_loaded_utc": datetime.utcnow().isoformat()
        }

        s3_key = (
            f"{RAW_PREFIX}"
            f"city={city_name}/"
            f"year={target_date.year}/"
            f"month={target_date.month:02d}/"
            f"nasa_power_{city_name}_{date_str}.json"
        )

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(data),
            ContentType="application/json"
        )

        print(f"Uploaded valid NASA JSON to s3://{BUCKET_NAME}/{s3_key}")

        uploaded_files.append(s3_key)

    return {
        "statusCode": 200,
        "message": "Latest available NASA POWER JSON files uploaded to S3",
        "files_uploaded": uploaded_files
    }
