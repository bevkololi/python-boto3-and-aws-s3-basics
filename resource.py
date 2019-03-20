import boto3
import uuid

s3_client = boto3.client('s3')

s3_resource = boto3.resource('s3')


def create_bucket_name(bucket_prefix):
    # The generated bucket name must be between 3 and 63 chars long
    return ''.join([bucket_prefix, str(uuid.uuid4())])


def create_bucket(bucket_prefix, s3_connection):
    session = boto3.session.Session()
    current_region = session.region_name
    bucket_name = create_bucket_name(bucket_prefix)
    bucket_response = s3_connection.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
            'LocationConstraint': current_region})
    return bucket_name, bucket_response


def create_temp_file(size, file_name, file_content):
    random_file_name = ''.join([str(uuid.uuid4().hex[:6]), file_name])
    with open(random_file_name, 'w') as f:
        f.write(str(file_content) * size)
    return random_file_name


def copy_to_bucket(bucket_from_name, bucket_to_name, file_name):
    copy_source = {
        'Bucket': bucket_from_name,
        'Key': file_name
    }
    s3_resource.Object(bucket_to_name, file_name).copy(copy_source)


def delete_all_objects(bucket_name):
    res = []
    bucket=s3_resource.Bucket(bucket_name)
    for obj_version in bucket.object_versions.all():
        res.append({'Key': obj_version.object_key,
                    'VersionId': obj_version.id})
    print(res)
    bucket.delete_objects(Delete={'Objects': res})


# creating a bucket using a client
# s3_resource.meta.client refers to the client
first_bucket_name, first_response = create_bucket(
    bucket_prefix='firstpythonbucket', s3_connection=s3_resource.meta.client)
print(first_response)
# creating a bucket using a resource
second_bucket_name, second_response = create_bucket(
    bucket_prefix='secondpythonbucket', s3_connection=s3_resource)
print(second_response)

# creating your first file
first_file_name = create_temp_file(300, 'firstfile.txt', 'f')
print(first_file_name)

# creating a bucket and object instances
first_bucket = s3_resource.Bucket(name=first_bucket_name)
first_object = s3_resource.Object(
    bucket_name=first_bucket_name, key=first_file_name
)

# uploading a file using object instance
s3_resource.Object(first_bucket_name, first_file_name).upload_file(
    Filename=first_file_name
)

# upload file using first object instance
first_object.upload_file(first_file_name)

# uploading a file using bucket instance
s3_resource.Bucket(first_bucket_name).upload_file(
    Filename=first_file_name, Key=first_file_name)

# uploading a file using the client
s3_resource.meta.client.upload_file(
    Filename=first_file_name, Bucket=first_bucket_name,
    Key=first_file_name
)

# downloading a file
s3_resource.Object(first_bucket_name, first_file_name).download_file(
    f'tmp/{first_file_name}')

# copy an object between buckets
copy_to_bucket(first_bucket_name, second_bucket_name, first_file_name)

# deleting an object
s3_resource.Object(second_bucket_name, first_file_name).delete()

# Using Access Control Lists ACL
second_file_name = create_temp_file(400, 'secondfile.txt', 's')
second_object = s3_resource.Object(first_bucket.name, second_file_name)
second_object.upload_file(second_file_name, ExtraArgs={
                          'ACL': 'public-read'})

# to get the ObjectAcl instance
second_object_acl = second_object.Acl()

# To see who has access to your object
print(second_object_acl.grants)

# one can make the object private again
second_object_acl.put(ACL='private')
print(second_object_acl.grants)

# protect your file using encryption during file upload
third_file_name = create_temp_file(300, 'thirdfile.txt', 't')
third_object = s3_resource.Object(first_bucket_name, third_file_name)
third_object.upload_file(third_file_name, ExtraArgs={
                         'ServerSideEncryption': 'AES256'})


# Traversals
# To give a complete list of buckets
for bucket in s3_resource.buckets.all():
    print(bucket.name)

# to give a complete list of buckets using the client
for bucket_dict in s3_resource.meta.client.list_buckets().get('Buckets'):
    print(bucket_dict['Name'])

# get object from bucket, i.e a lightweight representation of the object
for obj in first_bucket.objects.all():
    print(obj.key)

# to get the missing attributes of the above object
for obj in first_bucket.objects.all():
    subsrc = obj.Object()
    print(obj.key, obj.storage_class, obj.last_modified,
    ubsrc.version_id, subsrc.metadata)

deleting a non-empty bucket
delete_all_objects(first_bucket_name)

deleting buckets
s3_resource.Bucket(first_bucket_name).delete()

delete bucket using the client version
s3_resource.meta.client.delete_bucket(Bucket=second_bucket_name)