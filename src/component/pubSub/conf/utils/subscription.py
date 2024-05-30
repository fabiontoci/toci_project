from google.cloud import pubsub_v1

# Imposta il nome del progetto e del topic
project_id = 'training-gcp-309207'
topic_id = 'toci_topic'

# Crea il nome del topic nel formato giusto
topic_name = f"projects/{project_id}/topics/{topic_id}"

# Crea il nome della sottoscrizione
subscription_name = 'first-subscription'

# Crea un cliente Pub/Sub
subscriber = pubsub_v1.SubscriberClient()

# Crea la sottoscrizione
subscription_path = subscriber.subscription_path(project_id, subscription_name)
topic_path = subscriber.topic_path(project_id, topic_id)
subscription = subscriber.create_subscription(
    request={"name": subscription_path, "topic": topic_path}
)

print(f"Sottoscrizione creata: {subscription}")
