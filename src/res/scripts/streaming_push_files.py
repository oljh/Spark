import ibm_boto3
from ibm_botocore.client import Config, ClientError
import sys
import time
import json


def copy_file(cos_resource, src_bucket, src_key, tgt_bucket, tgt_key):
    """
    Copy file
    """
    print("Copying item: {0}/{1}".format(src_bucket, src_key))
    try:
        copy_file = {
            'Bucket': src_bucket,
            'Key': src_key
        }
        tgt_bucket = cos_resource.Bucket(tgt_bucket)
        tgt_obj = tgt_bucket.Object(tgt_key)
        tgt_obj.copy(copy_file)
        print("Item: {0}/{1} copied!".format(tgt_bucket, tgt_key))
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to copy item: {0}".format(e))


def delete_items(cos_bucket, items_list):
    try:
        response = cos_bucket.delete_objects(
            Delete={
                'Objects': items_list
            }
        )
        print(json.dumps(response.get("Deleted"), indent=4))
        return response
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to copy item: {0}".format(e))


src_bucket_name = 'big-data-education'
tgt_bucket_name = 'big-data-education'
user_name = sys.argv[1]

cos_resource = ibm_boto3.resource("s3",
                                  endpoint_url='https://s3.eu-de.cloud-object-storage.appdomain.cloud',
                                  ibm_api_key_id='i_UWu_ZyXMzkA200kUITchw5jteQPU5B2g4-Jf5rMzKL',
                                  ibm_service_instance_id='crn:v1:bluemix:public:cloud-object-storage:global:a/ea663257864d0a8ca602fa31477df840:ee57a94c-325a-4537-9637-612b422e41e1::',
                                  ibm_auth_endpoint='https://iam.ng.bluemix.net/oidc/token',
                                  config=Config(signature_version="oauth")
                                  )

cos_bucket = cos_resource.Bucket(src_bucket_name)
objects = cos_bucket.objects.filter(Prefix='education/spark/streaming_data/')
objects_to_delete = cos_bucket.objects.filter(Prefix='users/' + str(user_name) + '/spark/streaming_data/')

if len(list(objects_to_delete)) > 1000:
    print('Number of items in the bucket folder more then 1000!')
    sys.exit(1)

if len(list(objects_to_delete)) != 0:
    items_list = [{'Key': obj.key} for obj in objects_to_delete]
    response = delete_items(cos_bucket, items_list)


for obj in list(objects)[1:]:
    src_key = obj.key
    tgt_key = 'users/' + str(user_name) + '/spark/streaming_data/' + src_key.split('/')[-1]
    copy_file(cos_resource, src_bucket_name, src_key, tgt_bucket_name, tgt_key)
    time.sleep(15)


