import os
import time
import requests
import json
import subprocess
import pymongo
from requests.auth import HTTPDigestAuth
from pymongo.mongo_client import MongoClient

# Load environment variables
group_id = os.getenv('GROUPID')
public_key = os.getenv('PUBLICKEY')
private_key = os.getenv('PRIVATEKEY')
atlas_pw = os.getenv('ATLASPW')

# Headers for requests
headers = {
    'Accept': 'application/vnd.atlas.2024-07-18+json',
    'Content-Type': 'application/json'
}

# Current time in Unix epoch format
time_now = int(time.time())

# Cluster name
cluster_name = f"Sample-{time_now}"

# Cluster creation body
cluster_body = {
    "clusterType": "REPLICASET",
    "name": cluster_name,
    "replicationSpecs": [
        {
            "regionConfigs": [
                {
                    "analyticsAutoScaling": {
                        "autoIndexing": {
                            "enabled": False
                        },
                        "compute": {
                            "enabled": True,
                            "maxInstanceSize": "M20",
                            "minInstanceSize": "M10",
                            "scaleDownEnabled": True
                        },
                        "diskGB": {
                            "enabled": True
                        }
                    },
                    "analyticsSpecs": {
                        "instanceSize": "M10",
                        "nodeCount": 0
                    },
                    "autoScaling": {
                        "autoIndexing": {
                            "enabled": False
                        },
                        "compute": {
                            "enabled": True,
                            "maxInstanceSize": "M40",
                            "minInstanceSize": "M10",
                            "scaleDownEnabled": True
                        },
                        "diskGB": {
                            "enabled": True
                        }
                    },
                    "electableSpecs": {
                        "instanceSize": "M10",
                        "nodeCount": 3
                    },
                    "hiddenSecondarySpecs": {
                        "instanceSize": "M10",
                        "nodeCount": 0
                    },
                    "priority": 7,
                    "providerName": "GCP",
                    "readOnlySpecs": {
                        "instanceSize": "M10",
                        "nodeCount": 0
                    },
                    "regionName": "EASTERN_US"
                }
            ],
            "zoneName": "Zone 1"
        }
    ]
}

# Create the cluster
create_cluster_url = f"https://cloud.mongodb.com/api/atlas/v2/groups/{group_id}/clusters?pretty=true"
response = requests.post(create_cluster_url, headers=headers, auth=HTTPDigestAuth(public_key, private_key), data=json.dumps(cluster_body))

if response.status_code == 201:
    print(f"Cluster {cluster_name} created successfully.")
else:
    print(f"Failed to create cluster: {response.text}")
    exit(1)

# Poll for cluster to become IDLE
poll_url = f"https://cloud.mongodb.com/api/atlas/v2/groups/{group_id}/clusters"
start_time = time.time()

while True:
    response = requests.get(poll_url, headers=headers, auth=HTTPDigestAuth(public_key, private_key))
    clusters = response.json().get('results', [])
    
    cluster = next((c for c in clusters if c['name'] == cluster_name), None)
    
    if cluster:
        state_name = cluster['stateName']
        if state_name == 'IDLE':
            print(f"Cluster {cluster_name} is now IDLE.")
            break
        else:
            elapsed_time = time.time() - start_time
            print(f"{elapsed_time:.2f} seconds elapsed. Cluster state: {state_name}")
    else:
        print(f"Cluster {cluster_name} not found in the list of clusters.")
    
    time.sleep(10)

# Load sample data
mongouri = f"mongodb+srv://tom:{atlas_pw}@{cluster_name}.fgc5a.mongodb.net/"
dbname = "education"
collectionname = "student_grades"

mongoimport_command = [
    "mongoimport",
    "--uri", mongouri,
    "--drop",
    "--db", dbname,
    "--collection", collectionname,
    "--file", "student_grades.json",
    "--jsonArray"
]

try:
    subprocess.run(mongoimport_command, check=True)
    print("Sample data loaded successfully.")
except subprocess.CalledProcessError as e:
    print(f"Failed to load sample data: {e}")
    exit(1)

#Run agg pipeline to correct data in collection
# Connect to the MongoDB server
client = MongoClient(mongouri)
db = client.education

# Find the latest date_completed
max_date_pipeline = [
    {"$sort": {"date_completed": -1}},
    {"$limit": 1},
    {"$project": {"max_date_completed": "$date_completed", "_id": 0}}
]
maxdate_result = list(db.student_grades.aggregate(max_date_pipeline))
if not maxdate_result:
    print("No records found in student_grades.")
else:
    maxdate = maxdate_result[0]["max_date_completed"]

    # Define the pipeline for updating dates
    pipeline = [
        {"$addFields": {
            "diff": {
                "$dateDiff": {
                    "startDate": maxdate,
                    "endDate": "$$NOW",
                    "unit": "day"
                }
            }
        }},
        {"$set": {
            "date_assigned": {
                "$dateAdd": {
                    "startDate": "$date_assigned",
                    "unit": "day",
                    "amount": {"$subtract": ["$diff", 1]}
                }
            },
            "date_completed": {
                "$cond": {
                    "if": {"$eq": ["$status", "complete"]},
                    "then": {
                        "$dateAdd": {
                            "startDate": "$date_completed",
                            "unit": "day",
                            "amount": {"$subtract": ["$diff", 1]}
                        }
                    },
                    "else": None
                }
            }
        }},
        {"$unset": "diff"}
    ]

    # Update the documents
    result = db.student_grades.update_many({}, pipeline)
    print(result.raw_result)
# Create an index on date_completed
db.student_grades.create_index("date_completed")

# Create online archive
online_archive_url = f"https://cloud.mongodb.com/api/atlas/v2/groups/{group_id}/clusters/{cluster_name}/onlineArchives"
online_archive_body = {
    "collName": "student_grades",
    "collectionType": "STANDARD",
    "criteria": {
        "type": "DATE",
        "dateField": "date_completed",
        "dateFormat": "ISODATE",
        "expireAfterDays": 30
    },
    "dataExpirationRule": {
        "expireAfterDays": 7
    },
    "dataProcessRegion": {
        "cloudProvider": "AWS",
        "region": "US_EAST_1"
    },
    "dbName": "education",
    "partitionFields": [
        {
            "fieldName": "date_completed",
            "order": 0
        }
    ],
    "schedule": {
        "type": "DEFAULT"
    }
}

response = requests.post(online_archive_url, headers=headers, auth=HTTPDigestAuth(public_key, private_key), data=json.dumps(online_archive_body))

if response.status_code == 200:
    online_archive_id = response.json()['_id']
    print(f"Online archive created with ID: {online_archive_id}")
else:
    print(f"Failed to create online archive: {response.text}")
    exit(1)

POLL_ARCHIVE_URL = f"https://cloud.mongodb.com/api/atlas/v2/groups/{group_id}/clusters/{cluster_name}/onlineArchives"

# Polling loop for 'state' field
while True:
    try:
        response = requests.get(POLL_ARCHIVE_URL, headers=headers, auth=HTTPDigestAuth(public_key, private_key))
        response.raise_for_status()
        response_json = response.json()

        if 'results' in response_json and response_json['results']:
            state = response_json['results'][0]['state']
            if state == 'ACTIVE':
                print("State is ACTIVE.")
                break
        else:
            print(f"No 'results' key found in response or 'results' is empty: {response_json}")
    except requests.RequestException as e:
        print(f"An error occurred while checking state: {e}")

    print("State is not ACTIVE yet. Retrying in 10 seconds...")
    time.sleep(10)  # Delay between polls

# Polling loop for 'lastArchiveRun.endDate' field
while True:
    try:
        response = requests.get(POLL_ARCHIVE_URL, headers=headers, auth=HTTPDigestAuth(public_key, private_key))
        response.raise_for_status()
        response_json = response.json()

        if 'results' in response_json and response_json['results']:
            last_archive_run = response_json['results'][0].get('lastArchiveRun', {})
            end_date = last_archive_run.get('endDate')
            if end_date:
                print(f"Last archive run end date: {end_date}")
                break
        else:
            print(f"No 'results' key found in response or 'results' is empty: {response_json}")
    except requests.RequestException as e:
        print(f"An error occurred while checking last archive run: {e}")

    print("End date is not available yet. Retrying in 10 seconds...")
    time.sleep(10)  # Delay between polls

# Query online archive
archived_cluster_uri = f"mongodb://tom:{atlas_pw}@atlas-online-archive-{online_archive_id}-fgc5a.a.query.mongodb.net/?ssl=true&authSource=admin"

client = pymongo.MongoClient(archived_cluster_uri)
db = client['education']
collection = db['student_grades']

archived_doc_count = collection.count_documents({})
print(f"Number of documents in the online archive: {archived_doc_count}")

# Query original cluster
original_cluster_uri = f"mongodb+srv://tom:{atlas_pw}@{cluster_name}.fgc5a.mongodb.net/"
client = pymongo.MongoClient(original_cluster_uri)
db = client['education']
collection = db['student_grades']

original_doc_count = collection.count_documents({})
print(f"Number of documents in the original cluster: {original_doc_count}")