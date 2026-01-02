from fastapi import FastAPI
from influxdb_client import InfluxDBClient
import pandas as pd
from google import genai

def InfluxDB_Operation():
    # Connection parameters
    url = "https://us-east-1-1.aws.cloud2.influxdata.com"
    api_token = "Fgq6swEk4qUl01OuayP4NH5pv704rCgz9qhFO7dT5Fyc1KRbdVTQH9wFsWCjz9En4GX0WzBLwp52rMn34HavqQ=="
    org = "MyAI/ML_Workspace"
    bucket = "Crop-sensor-data"

    # Client creation
    Client = InfluxDBClient(url=url, token=api_token, org=org)

    query = f'''
    from(bucket: "{bucket}")
      |> range(start: -7d)
      |> filter(fn: (r) => r._measurement == "Crop-sensor-data")
    '''
    query_api = Client.query_api()
    results = query_api.query(query)

    records = []
    for table in results:
        for record in table.records:
            records.append({
                "time": record.get_time(),
                "location": record.values.get("location"),
                "crop_id": record.values.get("crop_id"),
                "field": record.get_field(),
                "value": record.get_value()
            })

    df_long = pd.DataFrame(records)

    df_wide = df_long.pivot_table(
        index=["time", "location", "crop_id"],
        columns="field",
        values="value"
    ).reset_index()

    df_final = df_wide[[
        "crop_id",
        "location",
        "temperature",
        "humidity",
        "npk_per",
        "micronutrient",
        "time"
    ]]

    # Sort by time (optional)
    df_final = df_final.sort_values("time").reset_index(drop=True)

    return df_final


app = FastAPI()

@app.get("/")
def summary():
    return {"message": "Hello World"}

@app.get("/crops")
def crops(
    location: str | None = None,
    crop_id: str | None = None
):
    df = InfluxDB_Operation()

    if location:
        df = df[df["location"] == location]

    if crop_id:
        df = df[df["crop_id"].astype(str) == str(crop_id)]

    row = df.iloc[0]

    prompt = f"""
    You are an agricultural AI expert.

    User query:
    {"Is this crop in healthy condition"}

    Crop sensor data:
    Crop ID: {row['crop_id']}
    Location: {row['location']}
    Temperature: {row['temperature']} °C
    Humidity: {row['humidity']} %
    NPK level: {row['npk_per']}
    Micronutrient level: {row['micronutrient']}
    Timestamp: {row['time']}

    Provide:
    1. One liner Crop condition summary
    """
    client = genai.Client(api_key="AIzaSyAwldv6ETs9Gfh_hSdWdCsr9Me51PW5Krk")
    response = client.models.generate_content(
        model="models/gemini-flash-latest",
        contents=prompt
    )
    return {
        "crop_id": row["crop_id"],
        "location": row["location"],
        "ai_insights": response
    }

