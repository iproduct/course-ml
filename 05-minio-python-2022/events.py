import json

from minio import Minio

def list_objects(client, bucket_name):
    objects = client.list_objects(bucket_name, recursive=True)
    for obj in objects:
        print('-->', obj.bucket_name, obj.object_name, obj.last_modified, obj.etag, obj.size, obj.content_type)

if __name__ == '__main__':
    # Init Minio client
    client = Minio('localhost:9000',
                   # access_key='AKIAIOSFODNN7EXAMPLE',
                   # secret_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                   access_key='admin',
                   secret_key='password',
                   secure=False)


    # Subscrine for notifications
    events = client.listen_bucket_notification('posts', None,
                                               None,
                                               ['s3:ObjectCreated:*',
                                                's3:ObjectRemoved:*',
                                                's3:ObjectAccessed:*'])
    for event in events:
        print("\nEvent received:")
        print(json.dumps(event, indent=4))


