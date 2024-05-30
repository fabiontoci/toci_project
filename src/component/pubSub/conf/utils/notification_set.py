import os
from google.cloud import storage

def configure_bucket_notifications(bucket_name):
    os.environ["GOOGLE_CLOUD_PROJECT"] = "training-gcp-309207"
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    topic = "projects/training-gcp-309207/topics/toci_topic"


    notification = bucket.notification(topic, ["OBJECT_FINALIZE"])
    notification.create()
    print(f"Il bucket {bucket_name} è stato configurato per inviare notifiche al topic di Pub/Sub {topic}")

    # Ottieni la politica IAM corrente del bucket
    policy = bucket.get_iam_policy(requested_policy_version=3)

    binding = {
        "role": "roles/pubsub.publisher",
        "members": [
            f"serviceAccount:service-{storage_client.project}@gcp-sa-cloud-storage.iam.gserviceaccount.com"
        ],
    }

    # Inizializziamo policy["bindings"] come una lista vuota se non esiste già
    if "bindings" not in policy:
        policy["bindings"] = []

    # Aggiungiamo il nuovo binding alla lista dei binding
    policy["bindings"].append(binding)

    # Applica la nuova politica IAM al bucket
    bucket.set_iam_policy(policy)

# Replace with your bucket name
bucket_name = 'bucket_toci'

configure_bucket_notifications(bucket_name)
