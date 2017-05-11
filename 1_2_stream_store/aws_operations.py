#!/usr/bin/python3
from boto.s3.connection import S3Connection
from boto.s3.key import Key

class awsOperations(object):
    '''
    Class to handle Amazon Web Services operations
    '''
    def __init__(self):
        self.s3_conn = None
        self.s3_key = []

    def connectS3(self, d_credentials, service_name):
        self.s3_conn = S3Connection(**d_credentials[service_name])

    def createS3Bucket(self, bucket_name, aws_user_email):
        self.bucket_name = bucket_name
        self.s3_bucket = self.s3_conn.create_bucket(bucket_name)
        self.s3_bucket.add_email_grant(permission='WRITE', email_address=aws_user_email)
        #self.s3_bucket = self.s3_conn.get_bucket(bucket_name)

    def createS3Key(self, key_name, upload_json):
        self.s3_key.extend(key_name)
        k = Key(self.s3_bucket)
        k.key = key_name
        k.content_type = 'json'
        k.set_contents_from_string(upload_json)
