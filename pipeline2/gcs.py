from google.cloud import storage

# creating a GCS client
client = storage.Client()

# https://console.cloud.google.com/storage/browser/[bucket-id]/
bucket = client.get_bucket('mgmt590-class')

# Push our file to the bucket
blob = bucket.blob('test.txt')

# Pull our file from the bucket
print(blob.download_as_string())

