from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,
                         BucketAlreadyExists)


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Initialize minioClient with an endpoint and access/secret keys.
    client = Minio('127.0.0.1:9000',
                        access_key='AKIAIOSFODNN7EXAMPLE',
                        secret_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                        secure=False)
    buckets = client.list_buckets()
    # client.remove_bucket("maylogs")
    buckets = client.list_buckets()

    for bucket in buckets:
        print(bucket.name, bucket.creation_date)
        # List all object paths in bucket that begin with my-prefixname.
        objects = client.list_objects(bucket.name, recursive=True)
        for obj in objects:
            print("---> ", obj.bucket_name, obj.object_name.encode('utf-8'), obj.last_modified,
                  obj.etag, obj.size, obj.content_type)
            # print(client.stat_object(obj.bucket_name, obj.object_name))

    # Make a bucket with the make_bucket API call.
    try:
           client.make_bucket("mybucket", location="us-east-1")
    except BucketAlreadyOwnedByYou as err:
           pass
    except BucketAlreadyExists as err:
           pass
    except ResponseError as err:
           raise
    else:
            # Put an object 'pumaserver_debug.log' with contents from 'pumaserver_debug.log'.
            try:
                   client.fput_object('mybucket', 'posts.json', './posts.json',  content_type='application/json')
            except ResponseError as err:
                   print(err)

    # Get a full object and prints the original object stat information.
    try:
        print(client.fget_object('mybucket', 'posts.json', './myobject.json'))
    except ResponseError as err:
        print(err)