from minio import Minio
from minio.commonconfig import CopySource, Tags
from minio.error import InvalidResponseError, MinioException
import json


def list_objects(client, bucket):
    objects = client.list_objects(bucket.name, recursive=True)
    for obj in objects:
        print('-->', obj.bucket_name, obj.object_name, obj.last_modified, obj.etag, obj.size, obj.content_type)
        # Get data of an object
        try:
            tags = client.get_object_tags(bucket_name=bucket.name, object_name=obj.object_name)
            print("TAGS: ", tags)
            response = client.get_object(bucket_name=bucket.name, object_name=obj.object_name)
            data = json.loads(response.data)
            print(json.dumps(data, indent=4))
        finally:
            response.close()
            response.release_conn()



if __name__ == '__main__':
    # Initialize Minio client
    client = Minio('127.0.0.1:9000', access_key='admin', secret_key='password', secure=False)

    # Make a new bucket
    try:
        client.make_bucket('users-2025', location='us-east-1')
    except MinioException as err:
        print('Minio exception:', err)
    except InvalidResponseError as err:
        raise



    # Add object to bucket
    try:
        result = client.fput_object('posts-2025', 'posts.json', './posts.json', content_type='application/json')
        print('!!! Result:')
        print(
            "created {0} object; etag: {1}, version-id: {2} => {3}:{4}".format(
                result.object_name, result.etag, result.version_id, result.bucket_name, result.object_name,
            ),
        )
        tags = Tags.new_object_tags()
        tags["Project"] = "Posts 2025"
        tags["User"] = "georgi"
        client.set_object_tags('posts-2025', 'posts.json', tags)
    except MinioException as err:
        print('Minio exception:', err)
    except InvalidResponseError as err:
        print('Invalid response error:', err)

    # Copy object under new name
    try:
        client.copy_object('posts-2025', 'new-posts.json', CopySource('posts-2025', 'posts.json'))
    except MinioException as err:
        print('Minio exception:', err)
    except InvalidResponseError as err:
        print('Invalid response error:', err)

    # list all buckets and objects
    buckets = client.list_buckets()
    for bucket in buckets:
        print(bucket.name, bucket.creation_date)
        list_objects(client, bucket)