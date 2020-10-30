from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou, BucketAlreadyExists)

def list_objects(client, bucket_name):
    objects = client.list_objects(bucket_name, recursive=True)
    for obj in objects:
        print('-->', obj.bucket_name, obj.object_name, obj.last_modified, obj.etag, obj.size, obj.content_type)

if __name__ == '__main__':
    # Init Minio client
    client = Minio('localhost:9000',
                   access_key='AKIAIOSFODNN7EXAMPLE',
                   secret_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                   secure=False)
    # client.remove_bucket(bucket.name)

    # Try to create bucket
    try:
        client.make_bucket('posts-bucket')
    except BucketAlreadyOwnedByYou as err:
        pass
    except BucketAlreadyExists as err:
        pass
    except ResponseError as err:
        raise

    # Try to put object
    try:
        client.fput_object('posts-bucket', 'posts8.json', './posts.json', content_type="application/json")
    except ResponseError as err:
        print(err)

    # List buckets
    buckets = client.list_buckets()
    # for bucket in buckets:
    #     print(bucket.name, bucket.creation_date)
    #     # List all files in bucket
    #     list_objects(client, bucket.name)
        # try:
        #     print(client.fget_object(bucket.name, obj.object_name, './results/' + obj.object_name))
        # except ResponseError as err:
        #     print(err)

    #Copy object with new name
    try:
        client.copy_object('posts-bucket', 'new-posts9.json', '/posts-bucket/posts.json')
    except ResponseError as err:
        print(err)
    list_objects(client, 'posts-bucket')
