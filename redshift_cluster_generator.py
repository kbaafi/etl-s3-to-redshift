import boto3 as aws
import configparser
import json
from botocore.exceptions import ClientError

class RedshiftClusterGenerator():
    def __init__(self,configFileAddr,useExistingRole,useExistingVpcSettings):
        '''
        Instatiates a RedshiftClusterGenerator with the configurations from 
        a config file and access-to-S3 flag.

        Args:
            config_file_addr    : str
                address of the config file
            needsAccessToS3     : boolean
                flag specifying whether to grant access to S3 to the Redshift Cluster
        '''
        try:
            config = configparser.ConfigParser()
            config.read_file(open(configFileAddr))
            self.configFile = configFileAddr
            self.configFileOK = True
        except Exception as e:
            print("\nCould not read the config file:")
            print(e)
            print("\n")
            self.configFileOK = False
            return

        try:
            self.key                    = config.get('AWS','KEY')
            self.secret                 = config.get('AWS','SECRET')
            self.region                 = config.get('AWS','REGION')
            self.db                     = {}
            self.db['ClusterType']      = config.get("DWH","DWH_CLUSTER_TYPE")
            self.db['NumNodes']         = int(config.get("DWH","DWH_NUM_NODES"))
            self.db['NodeType']         = config.get("DWH","DWH_NODE_TYPE")
            self.db['ClusterID']        = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
            self.db['Name']             = config.get("DWH","DWH_DB")
            self.db['AdminUsername']    = config.get("DWH","DWH_DB_USER")
            self.db['Password']         = config.get("DWH","DWH_DB_PASSWORD")
            self.db['Port']             = int(config.get("DWH","DWH_DB_PORT"))
            self.db['S3Role']           = config.get("DWH","DWH_IAM_ROLE_NAME")
            self.awsClusterProperties        = None
            self.outIP                  = config.get("LOCAL","OUT_IP")
            self.useExistingS3Role      = useExistingRole
            self.useExistingVpcSettings = useExistingVpcSettings

            self.s3Client = aws.resource('s3',aws_access_key_id = self.key,
                aws_secret_access_key = self.secret)

            self.iamClient = aws.client('iam', aws_access_key_id = self.key,
                aws_secret_access_key = self.secret,
                region_name = self.region )
            
            self.redshiftClient = aws.client('redshift', aws_access_key_id = self.key,
                aws_secret_access_key = self.secret,
                region_name = self.region )
            
            self.ec2Client = aws.resource('ec2',aws_access_key_id = self.key,
                aws_secret_access_key = self.secret,
                region_name = self.region )
            
        except Exception as e:
            print(e)
            return

    def createS3AccessRole(self):
        '''
        Creates a redshift role and attaches the 
        S3 read only access
        
        Args:
            self:
        Returns:
            roleArn: AWS Arn of S3 read only role
        
        '''
        print('Access to S3: Creating a new IAM Role')

        roleArn = None
        
        try:       
            S3AccessRole = self.iamClient.create_role(
                Path = '/',
                RoleName = self.db['S3Role'],
                Description = 'Allows Redshift clusters to access S3 buckets',
                AssumeRolePolicyDocument = json.dumps({
                    'Statement': [{
                        'Action': 'sts:AssumeRole',
                        'Effect': 'Allow',
                        'Principal': {
                            'Service': 'redshift.amazonaws.com'
                        }
                    }],
                    'Version': '2012-10-17'
            }))

            print('Access to S3: Attaching Policy')
            self.iamClient.attach_role_policy(RoleName = self.db['S3Role'],
                    PolicyArn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                    )['ResponseMetadata']['HTTPStatusCode']

            roleArn = S3AccessRole['Role']['Arn']

        except self.iamClient.exceptions.EntityAlreadyExistsException:
            if(self.useExistingS3Role == True):
                S3AccessRole = self.iamClient.get_role(RoleName = self.db["S3Role"])
                roleArn = S3AccessRole['Role']['Arn']
        return roleArn

    def generateRedshiftCluster(self):
        '''
        Creates a Redshift cluster using the properties of this class

        Parameters:
            None
        Returns:
            void
        '''
        
        try:
            # create the role in AWS for Redshift and
            # assign the policy to the new role
            print("Creating the access role for Redshift")
            S3ReadOnlyArn = self.createS3AccessRole()
        except ClientError as e:
            print(e)
            return

        print("Initiating request to AWS for Redshift Cluster")
        try:   
            # create the cluster
            self.redshiftClient.create_cluster(        
                ClusterType = self.db['ClusterType'],
                NodeType = self.db['NodeType'],
                NumberOfNodes = self.db['NumNodes'],
                DBName =self.db['Name'],
                ClusterIdentifier = self.db['ClusterID'],
                MasterUsername = self.db['AdminUsername'],
                MasterUserPassword = self.db['Password'],
                IamRoles = [S3ReadOnlyArn]
            )
        except Exception as e:
            print(e)
            return

        cluster_available_waiter = self.redshiftClient.get_waiter('cluster_available')
        

        print("Waiting for AWS to complete Redshift setup. Go drink some coffee or disturb a co-worker")
        
        try:
            cluster_available_waiter.wait(ClusterIdentifier = self.db['ClusterID'])
            print("Cluster ready")
            
            self.awsClusterProperties = self.redshiftClient.describe_clusters(ClusterIdentifier=self.db['ClusterID'])['Clusters'][0]
            if(self.awsClusterProperties is not None):
                self.setupVPCConnectivity()
                
                self.dwHost       = self.awsClusterProperties['Endpoint']['Address']
                self.dwRoleArn    = self.awsClusterProperties['IamRoles'][0]['IamRoleArn']

                self.saveDBConfigurations("DWH","DWH_ENDPOINT",self.dwHost )
                self.saveDBConfigurations("DWH","DWH_S3_IAM_ARN",self.dwRoleArn)
            else:
                print("\nRedshift cluster was not created. Cannot proceed further")
                print(".........................................................")
        except Exception as e:
            print(e)
            return

        


    def setupVPCConnectivity(self):
        '''
        Allows access between the Redshift cluster and a specified ip address
        in the configuration file
        
        Returns: void
        '''
        try:
            print("Setting up your network access to the cluster.......")
            
            vpc = self.ec2Client.Vpc(id=self.awsClusterProperties['VpcId'])
            defaultSecurityGroup = list(vpc.security_groups.all())[0]
            
            defaultSecurityGroup.authorize_ingress(
                GroupName= defaultSecurityGroup.group_name,
                CidrIp=self.outIP,  
                IpProtocol='TCP', 
                FromPort=int(self.db["Port"]),
                ToPort=int(self.db["Port"])
            )
            print("Network settings complete")
        except ClientError as error :
            if 'InvalidPermission.Duplicate' in str(error):
                if(self.useExistingVpcSettings==False):
                    raise Exception("Alert: A duplicate network connection exists and\
                         you can use it to connect to your database")
        except Exception as e:
            print(e)
        return


    def saveDBConfigurations(self,section,option,value):
        '''
        Saves the endpoint address and Iam role arn into the config
        file after the database has been created for later use

        Args:
            section: str
                The section of the config file to which this data is to be saved
            option: str
                The option being saved to
            value: str
                The value of the option
        Returns:
            void
        '''
        config = configparser.ConfigParser()
        config.read_file(open(self.configFile))

        def add_option():
            if config.has_option(section,option)==False:
                config.set(section,option,value)
            else:
                print("An option with the same values exist. Please consider entering the values manually")
                print("Enter these values into your config file:....")
                print(("Section: {} ").format(section))
                print(("Option: {}, Value: {} ").format(option,value))

        if(config.has_section(section)==False):
            config.add_section(section)
        add_option()

        with open(self.configFile,"w") as f:
            config.write(f)     