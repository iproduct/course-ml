

from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou, BucketAlreadyExists)

if __name__ == '__main__':
    # Init Minio client
    client = Minio('localhost:9000',
                   access_key='AKIAIOSFODNN7EXAMPLE',
                   secret_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                   secure=False)
    # client.remove_bucket(bucket.name)

    # Try to create bucket
    try:
        client.make_bucket("posts-bucket")
    except BucketAlreadyOwnedByYou as err:
        pass
    except BucketAlreadyExists as err:
        pass
    except ResponseError as err:
        raise

    # Try to put object
    try:
        client.fput_object('posts-bucket', 'posts.json', './posts.json', content_type="application/json")
    except ResponseError as err:
        print(err)

    # List buckets
    buckets = client.list_buckets()
    for bucket in buckets:
        print(bucket.name, bucket.creation_date)


