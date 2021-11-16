from minio import Minio
from minio.commonconfig import CopySource
from minio.error import (InvalidResponseError, MinioException)


def list_objects(client, bucket_name):
    objects = client.list_objects(bucket_name, recursive=True)
    for obj in objects:
        print('-->', obj.bucket_name, obj.object_name, obj.last_modified, obj.etag, obj.size, obj.content_type)


if __name__ == "__main__":
    # Init Minio client
    client = Minio('localhost:9000',
                   access_key='admin',
                   secret_key='password',
                   secure=False)
    try:
        client.make_bucket('posts-bucket')
    except MinioException as err:
        print("Minio exception:", err)
    except InvalidResponseError as err:
        raise

    try:
        client.fput_object('posts-bucket', 'posts2.json', './posts.json', content_type='application/json')
    except MinioException as err:
        print("Minio exception:", err)
    except InvalidResponseError as err:
        print("Invalid response error:", err)

    for bucket in client.list_buckets():
        print(bucket.name, bucket.creation_date)
        list_objects(client, bucket.name)

    # Copy object with new name
    print("------ After Copy: ------------")
    try:
        client.copy_object('posts-bucket', 'postFromMyblogs.json', CopySource('myblogs', 'posts.json'))
    except MinioException as err:
        print("Minio exception:", err)
    except InvalidResponseError as err:
        print("Invalid response error:", err)
    list_objects(client, 'posts-bucket')


    #Get all objects from 'posts-bucket'
    for bucket in client.list_buckets():
        for obj in client.list_objects(bucket.name, recursive=True):
            try:
                print(client.fget_object(bucket.name, obj.object_name, './results/' + obj.object_name))
            except MinioException as err:
                print("Minio exception:", err)
            except InvalidResponseError as err:
                print("Invalid response error:", err)

