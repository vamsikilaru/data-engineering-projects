import boto3
from botocore.exceptions import ClientError
import configparser
import json
import logging
import logging.config
from pathlib import Path
import argparse
import time

#setting up logger,logger properties defined in logging.ini
logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")
logger = logging.getLogger(__name__)

#loading cluster config from dwh.cfg file
config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/dwh.cfg"))


def create_IAM_role(iam_client):
    role_name = config.get('IAM_ROLE','NAME')
    role_description = config.get('IAM_ROLE','DESCRIPTION')
    role_policy_arn = config.get('IAM_ROLE','POLICY_ARN')

    logging.info(f"Created IAM role with name : {role_name}, description :{role_description}, policy:{role_policy_arn}")
    # creating role
    role_policy_document = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": { "Service": [ "redshift.amazonaws.com" ] },
                    "Action": [ "sts:AssumeRole" ]
                }
            ]
        }
    )
    try:
        create_response = iam_client.create_role(
            Path = '/',
            RoleName = role_name,
            Description = role_description,
            AssumeRolePolicyDocument = role_policy_document
        )
        logger.debug(f"Got reposese from IAM client for creating role :{create_response}")
        logger.info(f"Role create response code : {create_response['ResponseMetadata']['HTTPStatusCode']}")
    except Exception as e:
        logger.error(f"Error occoured while created role {role_name} :{e}")
        return False
    
    try:
        policy_response = iam_client.attach_role_policy(
            RoleName = role_name,
            PolicyArn = role_policy_arn
        )
        logger.debug(f"Got response from IAM client for applying policy to role :{policy_response}")
        logger.info(f"Attach policy response code : {policy_response['ResponseMetadata']['HTTPStatusCode']}")
    except Exception as e:
        logger.error(f"Error occount while applying ARN Policy : {e}")
        return False

    return True if create_response['ResponseMetadata']['HTTPStatusCode'] == 200 and policy_response['ResponseMetadata']['HTTPStatusCode']==200 else False

def delete_IAM_role(iam_client):
    role_name = config.get('IAM_ROLE','NAME')
    existing_roles = [role['RoleName'] for role in iam_client.list_roles()['Roles']]
    if (role_name not in existing_roles):
        logger.info(f"Role {role_name} does not exist")
        return True

    logger.info(f"Processing deleted IAM role {role_name}")
    try:
        detach_response = iam_client.detach_role_policy(RoleName=role_name,PolicyArn=config.get('IAM_ROLE','POLICY_ARN'))
        logger.debug(f"Response to detach policy from IAM role : {detach_response}")
        logger.info(f"Detach policy response code: {detach_response['ResponseMetadata']['HTTPStatusCode']}")
        delete_reponse = iam_client.delete_role(RoleName=role_name)
        logger.debug(f"Response for deleting IAM role : {delete_reponse}")
        logger.info(f"Delete role response code : {delete_reponse['ResponseMetadata']['HTTPStatusCode']}")
        
    except Exception as e:
        logger.error(f"Exception occured while delteing the role :{e}")
        return False
    return True if detach_response['ResponseMetadata']['HTTPStatusCode']==200 and delete_reponse['ResponseMetadata']['HTTPStatusCode']==200 else False

def get_group(ec2_client,group_name):
    groups = \
        ec2_client.describe_security_groups(Filters=[{'Name':'group-name','Values':[group_name]}])[
            'SecurityGroups']
    return None if len(groups)==0 else groups[0]

def create_ec2_security_group(ec2_client):
    if get_group(ec2_client,config.get('SECURITY_GROUP','NAME')) is not None:
        logger.info("Group already exists")
        return True
    vpc_id = ec2_client.describe_security_groups()['SecurityGroups'][0]['VpcId']
    response = ec2_client.create_security_group(
        Description = config.get('SECURITY_GROUP','DESCRIPTION'),
        GroupName = config.get('SECURITY_GROUP','NAME'),
        VpcId = vpc_id,
        DryRun = False
    )
    logger.debug(f"Security Group response : {response}")
    logger.info(f"Response code for group creation :{response['ResponseMetadata']['HTTPStatusCode']}")
    logger.info("Authorizing security group ingress")
    ingress_response = ec2_client.authorize_security_group_ingress(
        GroupId = response['GroupId'],
            GroupName = config.get('SECURITY_GROUP','NAME'),
            FromPort = int(config.get('INBOUND_RULE','PORT_RANGE')),
            ToPort = int(config.get('INBOUND_RULE','PORT_RANGE')),
            CidrIp = config.get('INBOUND_RULE','CIDRIP'),
            IpProtocol = config.get('INBOUND_RULE','PROTOCOL'),
            DryRun = False
    )
    logger.debug(f"Security Group response : {ingress_response}")
    logger.info(f"Response code for group creation :{ingress_response['ResponseMetadata']['HTTPStatusCode']}")
    return True if response['ResponseMetadata']['HTTPStatusCode']==200 and ingress_response['ResponseMetadata']['HTTPStatusCode']==200 else False

def delete_ec2_security_group(ec2_client):
    group_name = config.get('SECURITY_GROUP','NAME')
    group = get_group(ec2_client,group_name)
    if (group is None):
        logger.info(f"Group {group_name} does not exist")
        return True
    try:
        response = ec2_client.delete_security_group(
            GroupId=group['GroupId'],
            GroupName=group_name,
            DryRun= False
        )
        logger.debug(f"Delete security group response : {response}")
        logger.info(f"Delete security group response code : {response['ResponseMetadata']['HTTPStatusCode']}")
    except Exception as e:
        logger.error(f"Error occoured while deleting group :{e}")
        return False
    return response['ResponseMetadata']['HTTPStatusCode']==200

def create_cluster(redshift_client,iam_role_arn,vpc_security_group_id):
    cluster_type = config.get('DWH','DWH_CLUSTER_TYPE')
    node_type = config.get('DWH','DWH_NODE_TYPE')
    num_nodes = int(config.get('DWH','DWH_NUM_NODES'))
    cluster_identifier  = config.get('DWH','DWH_CLUSTER_IDENTIFIER')
    db_name = config.get('DWH','DWH_DB')
    database_port = int(config.get('DWH','DWH_PORT'))
    master_username = config.get('DWH','DWH_DB_USER')
    master_user_password = config.get('DWH','DWH_DB_PASSWORD')

    security_group = config.get('SECURITY_GROUP','NAME')

    iam_role = None

    try:
        response = redshift_client.create_cluster(
            DBName = db_name,
            ClusterIdentifier=cluster_identifier,
            ClusterType = cluster_type,
            NodeType = node_type,
            NumberOfNodes = num_nodes,
            MasterUsername = master_username,
            MasterUserPassword = master_user_password,
            Port = database_port,
            VpcSecurityGroupIds = vpc_security_group_id,
            IamRoles =[iam_role_arn]
        )
        logger.debug(f"Cluser creation response :{response}")
        logger.info(f"Cluster creation reponse code : {response['ResponseMetadata']['HTTPStatusCode']}")
    except Exception as e:
        logger.error(f"Exception occured while creating cluster :{e}")
        return False
    return response['ResponseMetadata']['HTTPStatusCode']==200

def get_cluster_status(redshift_client,cluster_identifier):
    response = redshift_client.describe_clusters(ClusterIdentifier= cluster_identifier)
    cluster_status = response['Clusters'][0]['ClusterStatus']
    logger.info(f"Cluster status : {cluster_status.upper()}")
    return True if cluster_status.upper() in ('AVAILABLE','ACTIVE', 'INCOMPATIBLE_NETWORK', 'INCOMPATIBLE_HSM', 'INCOMPATIBLE_RESTORE', 'INSUFFICIENT_CAPACITY', 'HARDWARE_FAILURE') else False

def delete_cluster(redshift_client):
    cluster_identifier = config.get('DWH','DWH_CLUSTER_IDENTIFIER')
    if(len(redshift_client.describe_clusters()['Clusters'])==0):
        logger.info(f"Cluster {cluster_identifier} does not exist")
        return True
    try:
        while (not get_cluster_status(redshift_client,cluster_identifier)):
            logger.info("Can not delete cluster. Wating for the cluster to become Active")
            time.sleep(10)
        response = redshift_client.delete_cluster(ClusterIdentifier=cluster_identifier,SkipFinalClusterSnapshot=True)
        logger.debug(f"Cluster deleted reponse : {response}")
        logger.info(f"Cluster deleted response code : {response['ResponseMetadata']['HTTPStatusCode']}")
    except Exception as e:
        logger.error(f"Exception occured while deleting cluster :{e}")
        return False
    return response['ResponseMetadata']['HTTPStatusCode']==200

def boolean_parser(val):
    if val.upper() not in ['FALSE','TRUE']:
        logger.error(f"Invalid argument :{val}. Must be TRUE or FLASE")
        raise ValueError('Not a valid booean string')
    return val.upper() == 'TRUE'

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Redshift Cluster Infrastructure as a Code. It created IAM Role for the redshift, created securitu group and spin up redshift cluster")
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')
    required.add_argument("-c","--create",type=boolean_parser,metavar='',required=True,
            help="True or Flase. Create IAM roles, securitygroup and redshift cluster if it does not exist.")
    required.add_argument("-d", "--delete", type=boolean_parser, metavar='', required=True,
            help="True or False. Delete the roles, securitygroup and cluster. WARNING: Deletes the Redshift cluster, IAM role and security group.")
    optional.add_argument("-v","--verbosity",type=boolean_parser,metavar='',required=False,default=True,help="Increase output verbosity. Default set to DEBUG.")
    args =parser.parse_args()
    logger.info(f"ARGS : {args}")

    if args.verbosity:
        logger.setLevel(logging.DEBUG)
        logger.info("LOGGING LEVEL SET TO DEBUG")
    
    ec2 = boto3.client(service_name ='ec2', region_name ='us-west-1', aws_access_key_id=config.get('AWS','key'), aws_secret_access_key=config.get('AWS','SECRET'))

    s3 = boto3.client(service_name ='s3', region_name ='us-west-1', aws_access_key_id=config.get('AWS','key'), aws_secret_access_key=config.get('AWS','SECRET'))

    iam = boto3.client(service_name ='iam', region_name ='us-west-1', aws_access_key_id=config.get('AWS','key'), aws_secret_access_key=config.get('AWS','SECRET'))

    redshift = boto3.client(service_name ='redshift', region_name ='us-west-1', aws_access_key_id=config.get('AWS','key'), aws_secret_access_key=config.get('AWS','SECRET'))

    logger.info("Client set up done for all services")
    
    if(args.create):
        if(create_IAM_role(iam)):
            logger.info("IAM role created. Creating security group....")
            if(create_ec2_security_group(ec2)):
                logger.info("Security group created. Spinning up redshift cluster....")
                role_arn = iam.get_role(RoleName=config.get('IAM_ROLE','NAME'))['Role']['Arn']
                vpc_security_group_id = get_group(ec2,config.get('SECURITY_GROUP','NAME'))['GroupId']
                create_cluster(redshift,role_arn,[vpc_security_group_id])
            else:
                logger.error("Failed to create security group")
        else:
            logger.error("Failed to create IAM role")
    else:
        logger.error("Skip Creation")

    #clean up
    if(args.delete):
        delete_cluster(redshift)
        time.sleep(60)
        delete_ec2_security_group(ec2)
        delete_IAM_role(iam)