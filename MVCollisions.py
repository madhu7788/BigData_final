# Databricks notebook source
# MAGIC %pip install pymongo
# MAGIC

# COMMAND ----------

import requests

API_URL = "https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.json?accessType=DOWNLOAD"
local_path = "/tmp/nyc_collisions.json"

response = requests.get(API_URL)
with open(local_path, "wb") as f:
    f.write(response.content)

raw_df = spark.read.json(local_path)
display(raw_df)


# COMMAND ----------

import requests
import json

API_URL = "https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.json?accessType=DOWNLOAD"

response = requests.get(API_URL)
response.raise_for_status()

json_data = response.json()



# COMMAND ----------

c=json_data.get('meta').get('view').get('columns')
col=[col['name'] for col in c]

# COMMAND ----------

import pandas as pd
df = pd.DataFrame(json_data.get('data', []), columns=col)

# COMMAND ----------

df.head()

# COMMAND ----------

df=df.iloc[:,8:]
df.head()

# COMMAND ----------

import pymongo
import pandas as pd
from pymongo import MongoClient

# COMMAND ----------

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://madhu:terama@cluster0.zqtigde.mongodb.net/?retryWrites=true&w=majority&connectTimeoutMS=300000"
client = MongoClient(uri, server_api=ServerApi('1'))
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

# COMMAND ----------

import pymongo
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import AutoReconnect

db = client['bigdata']
collection = db['MVCollisions']

data = df.to_dict(orient='records')

try:
    batch_size = 10000
    for i in range(0, 100000, batch_size):
        batch = data[i : i + batch_size]
        collection.insert_many(batch, ordered=False)
except AutoReconnect as e:
    print(f"AutoReconnect error: {e}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    client.close()  

# COMMAND ----------

from pymongo import MongoClient
import pandas as pd
from pymongo.server_api import ServerApi

uri = "mongodb+srv://madhu:terama@cluster0.zqtigde.mongodb.net/?retryWrites=true&w=majority&connectTimeoutMS=300000"
client = MongoClient(uri, server_api=ServerApi('1'))

# COMMAND ----------

db = client['bigdata']
collection = db['MVCollisions']

MVCollisions_data = pd.DataFrame(list(collection.find()))

# COMMAND ----------

MVCollisions_data.info()

# COMMAND ----------

MVCollisions_data.columns

# COMMAND ----------

import pandas as pd

df = MVCollisions_data.copy()

# Drop MongoDB internal ID
df.drop(columns=["_id"], inplace=True)

# Shape
print("Rows:", df.shape[0])
print("Columns:", df.shape[1])

# Data types
print(df.dtypes)

# Missing values
missing = df.isnull().sum()
print(missing[missing > 0])


# COMMAND ----------

df.rename(columns={
    "CRASH DATE": "crash_date",
    "CRASH TIME": "crash_time",
    "BOROUGH": "borough",
    "ZIP CODE": "zip_code",
    "LATITUDE": "latitude",
    "LONGITUDE": "longitude",
    "ON STREET NAME": "on_street",
    "NUMBER OF PERSONS INJURED": "persons_injured",
    "NUMBER OF PERSONS KILLED": "persons_killed",
    "COLLISION_ID": "collision_id"
}, inplace=True)


# COMMAND ----------

df["borough"].fillna("UNKNOWN", inplace=True)
df["on_street"].fillna("UNKNOWN", inplace=True)
df["persons_injured"].fillna(0, inplace=True)
df["persons_killed"].fillna(0, inplace=True)


# COMMAND ----------

df["borough"] = df["borough"].str.strip().str.upper()
df["on_street"] = df["on_street"].str.strip().str.upper()


# COMMAND ----------

df["crash_date"] = pd.to_datetime(df["crash_date"], errors="coerce").dt.date

df["crash_timestamp"] = pd.to_datetime(
    df["crash_date"].astype(str) + " " + df["crash_time"],
    errors="coerce"
)


# COMMAND ----------

before = df.shape[0]
df.drop_duplicates(subset=["collision_id"], inplace=True)
after = df.shape[0]

print(f"Removed {before - after} duplicate records")


# COMMAND ----------

from pydantic import BaseModel
from typing import Optional
from datetime import datetime, date

class MVCollision(BaseModel):
    collision_id: int
    crash_date: date
    crash_timestamp: datetime
    borough: str
    zip_code: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    on_street: str
    persons_injured: int
    persons_killed: int


# COMMAND ----------

sample = df.head(1000).to_dict("records")

for row in sample:
    MVCollision(**row)

print("Schema validation passed")


# COMMAND ----------

if "_id" in df.columns:
    df.drop(columns=["_id"], inplace=True)


# COMMAND ----------

import pandas as pd

# Convert crash_date to datetime (not date)
df["crash_date"] = pd.to_datetime(df["crash_date"], errors="coerce")

# Convert crash_timestamp to native Python datetime
df["crash_timestamp"] = pd.to_datetime(
    df["crash_timestamp"], errors="coerce"
).dt.to_pydatetime()


# COMMAND ----------

numeric_cols = [
    "persons_injured",
    "persons_killed",
    "latitude",
    "longitude"
]

for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)


# COMMAND ----------

text_cols = ["borough", "on_street", "zip_code"]

for col in text_cols:
    df[col] = df[col].astype(str)


# COMMAND ----------

client = MongoClient(uri, server_api=ServerApi('1'))
db = client["bigdata"]
collection = db["MVCollisions_silver"]

from pymongo.errors import AutoReconnect

silver_data = df.to_dict(orient="records")

try:
    batch_size = 10000
    for i in range(0, len(silver_data), batch_size):
        batch = silver_data[i : i + batch_size]
        collection.insert_many(batch, ordered=False)

    print("Silver layer ingestion completed successfully")

except AutoReconnect as e:
    print(f"AutoReconnect error during Silver ingestion: {e}")

except Exception as e:
    print(f"Error during Silver ingestion: {e}")



# COMMAND ----------

print(
    "Silver collection count:",
    collection.count_documents({})
)


# COMMAND ----------

# Silver indexes
db.MVCollisions_silver.create_index("crash_date")
db.MVCollisions_silver.create_index("borough")
db.MVCollisions_silver.create_index("collision_id", unique=True)

# Gold indexes
db.MVCollisions_gold_borough.create_index("borough")
db.MVCollisions_gold_daily.create_index("crash_date")


# COMMAND ----------

agg_borough = (
    df.groupby("borough")
      .size()
      .reset_index(name="total_accidents")
)


# COMMAND ----------

agg_injuries = (
    df.groupby("borough")[["persons_injured", "persons_killed"]]
      .sum()
      .reset_index()
)


# COMMAND ----------

agg_daily = (
    df.groupby("crash_date")
      .size()
      .reset_index(name="daily_accidents")
)


# COMMAND ----------

db = client["bigdata"]

gold_borough_col = db["MVCollisions_gold_borough"]
gold_injury_col = db["MVCollisions_gold_injuries"]
gold_daily_col = db["MVCollisions_gold_daily"]


# COMMAND ----------

from pymongo.errors import AutoReconnect

def batch_insert(collection, data, batch_size=10000):
    try:
        records = data.to_dict(orient="records")
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            collection.insert_many(batch, ordered=False)
    except AutoReconnect as e:
        print(f"AutoReconnect error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


# COMMAND ----------

batch_insert(gold_borough_col, agg_borough)
batch_insert(gold_injury_col, agg_injuries)
batch_insert(gold_daily_col, agg_daily)

print("Gold layer ingestion completed")


# COMMAND ----------

print("Gold Borough Count:", gold_borough_col.count_documents({}))
print("Gold Injury Count:", gold_injury_col.count_documents({}))
print("Gold Daily Count:", gold_daily_col.count_documents({}))


# COMMAND ----------

import matplotlib.pyplot as plt

borough_df = pd.DataFrame(
    list(db.MVCollisions_gold_borough.find({}, {"_id": 0}))
)

plt.figure(figsize=(10, 5))
plt.bar(borough_df["borough"], borough_df["total_accidents"])
plt.title("Total Motor Vehicle Accidents by Borough")
plt.xlabel("Borough")
plt.ylabel("Total Accidents")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


# COMMAND ----------

injury_df = pd.DataFrame(
    list(db.MVCollisions_gold_injuries.find({}, {"_id": 0}))
)

injury_df.set_index("borough")[["persons_injured", "persons_killed"]] \
    .plot(kind="bar", stacked=True, figsize=(10, 5))

plt.title("Injuries vs Fatalities by Borough")
plt.xlabel("Borough")
plt.ylabel("Count")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


# COMMAND ----------

daily_df = pd.DataFrame(
    list(db.MVCollisions_gold_daily.find({}, {"_id": 0}))
)

daily_df["crash_date"] = pd.to_datetime(daily_df["crash_date"])
daily_df.sort_values("crash_date", inplace=True)

plt.figure(figsize=(12, 5))
plt.plot(daily_df["crash_date"], daily_df["daily_accidents"])
plt.title("Daily Motor Vehicle Accident Trend")
plt.xlabel("Date")
plt.ylabel("Number of Accidents")
plt.tight_layout()
plt.show()


# COMMAND ----------

street_df = pd.DataFrame(
    list(
        db.MVCollisions_silver.aggregate([
            {"$group": {"_id": "$on_street", "total_accidents": {"$sum": 1}}},
            {"$sort": {"total_accidents": -1}},
            {"$limit": 10}
        ])
    )
)

street_df.rename(columns={"_id": "on_street"}, inplace=True)

plt.figure(figsize=(10, 5))
plt.barh(street_df["on_street"], street_df["total_accidents"])
plt.gca().invert_yaxis()
plt.title("Top 10 Streets with Highest Accidents")
plt.xlabel("Total Accidents")
plt.ylabel("Street Name")
plt.tight_layout()
plt.show()


# COMMAND ----------

severity_df = pd.DataFrame(
    list(
        db.MVCollisions_silver.aggregate([
            {
                "$project": {
                    "severity": {
                        "$cond": [
                            {"$gt": ["$persons_killed", 0]},
                            "Fatal",
                            {
                                "$cond": [
                                    {"$gt": ["$persons_injured", 0]},
                                    "Injury",
                                    "No Injury"
                                ]
                            }
                        ]
                    }
                }
            },
            {"$group": {"_id": "$severity", "count": {"$sum": 1}}}
        ])
    )
)

severity_df.rename(columns={"_id": "severity"}, inplace=True)

plt.figure(figsize=(6, 6))
plt.pie(
    severity_df["count"],
    labels=severity_df["severity"],
    autopct="%1.1f%%",
    startangle=140
)
plt.title("Accident Severity Distribution")
plt.tight_layout()
plt.show()


# COMMAND ----------

monthly_df = pd.DataFrame(
    list(
        db.MVCollisions_silver.aggregate([
            {
                "$project": {
                    "month": {"$month": "$crash_timestamp"}
                }
            },
            {"$group": {"_id": "$month", "total_accidents": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ])
    )
)

monthly_df.rename(columns={"_id": "month"}, inplace=True)

plt.figure(figsize=(10, 5))
plt.plot(monthly_df["month"], monthly_df["total_accidents"], marker="o")
plt.title("Monthly Accident Trend")
plt.xlabel("Month")
plt.ylabel("Total Accidents")
plt.xticks(range(1, 13))
plt.tight_layout()
plt.show()
