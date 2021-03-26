import boto3
import configparser
from settings import config_file

config = configparser.ConfigParser()
config.read_file(open(config_file))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
DWH_REGION_NAME        = config.get("DWH","DWH_REGION_NAME")


def release() :
    """
    Utility function that automatically drops resources in redshift cluster.

    Parameters
    ----------
    No parameter

    Returns
    -------
    No return.
    """
    iam = boto3.client(
        'iam',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name=DWH_REGION_NAME
    )

    redshift = boto3.client(
        'redshift',
        region_name=DWH_REGION_NAME,
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )

    try:
        redshift.delete_cluster( 
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  
            SkipFinalClusterSnapshot=True
        )
    except Exception as e:
        print(e)

    try:
        iam.detach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME, 
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )
    except Exception as e:
        print(e)
           
    try: 
        iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    except Exception as e:
        print(e)



if __name__ == "__main__":

    release()




    