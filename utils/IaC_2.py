import pandas as pd
import boto3
import json
import os
import configparser
from botocore.exceptions import ClientError
import psycopg2
from settings import config_file, set_config_file


config = configparser.ConfigParser()
config.read_file(open(config_file))
KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_REGION_NAME        = config.get("DWH","DWH_REGION_NAME")


ec2 = boto3.resource(
    'ec2',
    region_name=DWH_REGION_NAME,
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET
)


redshift = boto3.client(
    'redshift',
    region_name=DWH_REGION_NAME,
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET
)


myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]


DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
set_config_file(config_file, DWH_ENDPOINT, DWH_ROLE_ARN)


try:
    print('Open an incoming TCP port to access the cluster endpoint')
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    defaultSg.authorize_ingress(
        GroupName='default',
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
except Exception as e:
    print(e)

    