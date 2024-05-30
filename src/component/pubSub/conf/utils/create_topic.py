import os

from google.cloud import pubsub_v1


def create_topic(project_id, topic_name):
    os.environ["GOOGLE_CLOUD_PROJECT"] = "training-gcp-309207"
    publisher = pubsub_v1.PublisherClient()
    topic_path = f"projects/{project_id}/topics/{topic_name}"

    topic = publisher.create_topic(request={"name": topic_path})

    print(f"Topic created: {topic}")
    return topic


# Replace with your project ID and desired topic name
project_id = 'nttdata-training-407416'
topic_name = 'toci_topic'

create_topic(project_id, topic_name)
