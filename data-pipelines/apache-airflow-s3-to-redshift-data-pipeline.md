
# Using Apache Airflow to build a S3-to-Redshift DAG
## Author: Shravan Kuchkula

## Introduction
At it's most basic definition a data pipeline describes “in code” a series of sequential data processing steps. Apache Airflow lets us build Data Pipeline using python code.

In this post, I will show how we can build a simple data pipeline that will take data from S3 and load it into RedShift using Apache Airflow.

## Part 1: Launch an AWS Redshift Cluster

### Step 1: Create an IAM user
In most cases, people are given an IAM user to work with. If you are doing this from scratch, then go into your AWS console and create a new IAM user.

- Create a new IAM user in your AWS account
- Give it `AdministratorAccess`, From `Attach existing policies directly` Tab
- Take note of the access key and secret 
- Edit the file `dwh-iac.cfg` in the same folder as this notebook
<font color='red'>
<BR>
[AWS]<BR>
KEY= YOUR_AWS_KEY<BR>
SECRET= YOUR_AWS_SECRET<BR>
<font/>


```python
import pandas as pd
import boto3
import json
```

### Step 2: Use configparser to read in the variables
In the `dwh.cfg` file, you will store your secrets and configuration files. This should not be checked into git.

The file looks like this:
```
[AWS]
KEY=<your-key>
SECRET=<your-secret>

[DWH]
DWH_CLUSTER_TYPE=multi-node
DWH_NUM_NODES=4
DWH_NODE_TYPE=dc2.large

DWH_IAM_ROLE_NAME=dwhRole
DWH_CLUSTER_IDENTIFIER=dwhCluster
DWH_DB=dwh
DWH_DB_USER=dwhuser
DWH_DB_PASSWORD=<password>
DWH_PORT=5439
```


```python
import configparser
config = configparser.ConfigParser()
config.read_file(open('dwh-iac.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

#(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", \
                   "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", \
                   "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER,\
                   DWH_DB, DWH_DB_USER, "XXX", DWH_PORT, DWH_IAM_ROLE_NAME]
             })
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Param</th>
      <th>Value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>DWH_CLUSTER_TYPE</td>
      <td>multi-node</td>
    </tr>
    <tr>
      <th>1</th>
      <td>DWH_NUM_NODES</td>
      <td>4</td>
    </tr>
    <tr>
      <th>2</th>
      <td>DWH_NODE_TYPE</td>
      <td>dc2.large</td>
    </tr>
    <tr>
      <th>3</th>
      <td>DWH_CLUSTER_IDENTIFIER</td>
      <td>dwhCluster</td>
    </tr>
    <tr>
      <th>4</th>
      <td>DWH_DB</td>
      <td>dwh</td>
    </tr>
    <tr>
      <th>5</th>
      <td>DWH_DB_USER</td>
      <td>dwhuser</td>
    </tr>
    <tr>
      <th>6</th>
      <td>DWH_DB_PASSWORD</td>
      <td>XXX</td>
    </tr>
    <tr>
      <th>7</th>
      <td>DWH_PORT</td>
      <td>5439</td>
    </tr>
    <tr>
      <th>8</th>
      <td>DWH_IAM_ROLE_NAME</td>
      <td>dwhRole</td>
    </tr>
  </tbody>
</table>
</div>



### Step 3: Create clients for EC2, S3, IAM, and Redshift
Here, we will be using `boto3` to get the resource handles to talk to S3, EC2, IAM and Redshift. Notice that for Redshift, you will be creating a client. I am choosing `us-west-2` as my region.


```python
import boto3

ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )

s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )

iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )

redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
```

### Step 4: Create an IAM role and assign it a policy so that it can read S3 bucket
- Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)


```python
from botocore.exceptions import ClientError

#1.1 Create the role, 
try:
    print("1.1 Creating a new IAM Role") 
    dwhRole = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
    )    
except Exception as e:
    print(e)
    
    
print("1.2 Attaching Policy")

iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']

print("1.3 Get the IAM role ARN")
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

print(roleArn)
```

    1.1 Creating a new IAM Role
    1.2 Attaching Policy
    1.3 Get the IAM role ARN
    arn:aws:iam::506140549518:role/dwhRole


### Step 5:  Create a Redshift Cluster and Launch it

- Create a RedShift Cluster
- For complete arguments to `create_cluster`, see [docs](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift.html#Redshift.Client.create_cluster)


```python
try:
    response = redshift.create_cluster(        
        #HW
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        #Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        
        #Roles (for s3 access)
        IamRoles=[roleArn]  
    )
except Exception as e:
    print(e)
```

### Step 6: *Describe* the cluster to see its status
- Run this block several times until the cluster status becomes `Available`


```python
def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", \
                  "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
prettyRedshiftProps(myClusterProps)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Key</th>
      <th>Value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ClusterIdentifier</td>
      <td>dwhcluster</td>
    </tr>
    <tr>
      <th>1</th>
      <td>NodeType</td>
      <td>dc2.large</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ClusterStatus</td>
      <td>available</td>
    </tr>
    <tr>
      <th>3</th>
      <td>MasterUsername</td>
      <td>dwhuser</td>
    </tr>
    <tr>
      <th>4</th>
      <td>DBName</td>
      <td>dwh</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Endpoint</td>
      <td>{'Address': 'dwhcluster.c4m4dhrmsdov.us-west-2.redshift.amazonaws.com', 'Port': 5439}</td>
    </tr>
    <tr>
      <th>6</th>
      <td>VpcId</td>
      <td>vpc-9a53d7e2</td>
    </tr>
    <tr>
      <th>7</th>
      <td>NumberOfNodes</td>
      <td>4</td>
    </tr>
  </tbody>
</table>
</div>



> **Note:** Make a note of <font color='red'> Cluster Endpoint and Role ARN </font>


```python
##
## DO NOT RUN THIS unless the cluster status becomes "Available"
##
DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
#print("DWH_ENDPOINT :: ", endpoint)
print("DWN_ENDPOINT :: ", DWH_ENDPOINT)
print("DWH_ROLE_ARN :: ", roleArn)
```

    DWN_ENDPOINT ::  dwhcluster.c4m4dhrmsdov.us-west-2.redshift.amazonaws.com
    DWH_ROLE_ARN ::  arn:aws:iam::506140549518:role/dwhRole


### Step 7: Create Security group inbound rule


```python
# Open an incoming  TCP port to access the cluster ednpoint
try:
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    
    defaultSg.authorize_ingress(
        GroupName= 'default',  
        CidrIp='0.0.0.0/0',  
        IpProtocol='TCP',  
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
except Exception as e:
    print(e)
```

    ec2.SecurityGroup(id='sg-b27b30f3')


## Part 2: Design your Data Pipeline.

Our DAG here is called `s3-to-redshift`. 

This pipeline is generated using the below code. There are 3 essential components of a data pipeline:
- **DAGs** : A DAG is made up of a sequence of operators.
- **Operators**: An operator is basically a task (the rectangle box). The operator makes use of Hooks to connect with external entities like S3, Redshift etc.
     * **PythonOperator**: This takes a callback function which defines the instructions that make up the task.
     * **PostgresOperator**: This takes a sql argument, which when given a SQL statement, it executes it in the task.
- **Hooks**: Think of a hook as getting the connection that you have defined in Airflow. (I describe this further down).

The structure of the pipeline itself is very easy to layout. Like so:
```
create_table >> copy_task
copy_task >> location_traffic_task
```

copy_task here is PythonOperator which takes `load_data_to_redshift` as the callback function.

![data-pipeline](images/data-pipeline.png)

### Code to generate the DAG

```python
import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

import sql_statements


def load_data_to_redshift(*args, **kwargs):
    # use AWS hook to get to aws connection id
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    
    # use PostgresHook to get to redshift connection id
    redshift_hook = PostgresHook("redshift")
    redshift_hook.run(sql_statements.COPY_ALL_TRIPS_SQL.format(credentials.access_key, credentials.secret_key))


dag = DAG(
    's3-to-redshift-dag',
    start_date=datetime.datetime.now()
)

create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TRIPS_TABLE_SQL
)

copy_task = PythonOperator(
    task_id='load_from_s3_to_redshift',
    dag=dag,
    python_callable=load_data_to_redshift
)

location_traffic_task = PostgresOperator(
    task_id="calculate_location_traffic",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.LOCATION_TRAFFIC_SQL
)

create_table >> copy_task
copy_task >> location_traffic_task
```
 

### Callbacks and context variables
It is worth mentioning here, that you can also pass context to the PythonOperator, which essentially allows it to access Airflow's runtime variables (context variables). 

```
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def hello_date(*args, **kwargs):
    print(f“Hello {kwargs[‘execution_date’]}”)

divvy_dag = DAG(...)
task = PythonOperator(
    task_id=’hello_date’,
    python_callable=hello_date,
    provide_context=True,
    dag=divvy_dag)
```
All the context variables: 
https://airflow.apache.org/macros.html


### Define S3 and Redshift connections in Airflow

On the left is the `S3 connection`. The Login and password are the IAM user's access key and secret key that you created in part 1. Basically, by using these credentials, we are able to read data from S3.

On the right is the `redshift connection`. These values can be easily gathered from your Redshift cluster in part 1.

![connections](images/connections.png)

### Define S3 variables for bucket and prefix (so that you don't have to hard code that in the code). Airflow maintains these for us. But we need to create it first.

![variables](images/variables.png)

## Part 3: Connect to cluster, load data and perform analysis

In the second part, we will be making use of the cluster we just launched in part 1. 

You can quickly validate using the redshift console that the tables are created:

![validate-on-redshift](images/validate-on-redshift.png)

Alternately, you can connect to the redshift cluster from jupyter notebook, as shown below:

Using sql extension, we can directly run SQL commands within jupyter notebook. 
- A single `%sql` means the query is a python string accessed using the dollar sign.
- A `%%sql` means the query is not a python string but can be multiline SQL statements 



### Load sql extension and connect to the cluster


```python
%load_ext sql
```


```python
conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
print(conn_string)
%sql $conn_string
```

    postgresql://dwhuser:Passw0rd@dwhcluster.c4m4dhrmsdov.us-west-2.redshift.amazonaws.com:5439/dwh





    'Connected: dwhuser@dwh'




```python
%%sql
SELECT * FROM station_traffic limit 10;
```

     * postgresql://dwhuser:***@dwhcluster.c4m4dhrmsdov.us-west-2.redshift.amazonaws.com:5439/dwh
    10 rows affected.





<table>
    <tr>
        <th>station_id</th>
        <th>station_name</th>
        <th>num_departures</th>
        <th>num_arrivals</th>
    </tr>
    <tr>
        <td>224</td>
        <td>Halsted St &amp; Willow St</td>
        <td>282</td>
        <td>277</td>
    </tr>
    <tr>
        <td>225</td>
        <td>Halsted St &amp; Dickens Ave</td>
        <td>419</td>
        <td>490</td>
    </tr>
    <tr>
        <td>59</td>
        <td>Wabash Ave &amp; Roosevelt Rd</td>
        <td>997</td>
        <td>1099</td>
    </tr>
    <tr>
        <td>197</td>
        <td>Michigan Ave &amp; Madison St</td>
        <td>535</td>
        <td>571</td>
    </tr>
    <tr>
        <td>461</td>
        <td>Broadway &amp; Ridge Ave</td>
        <td>180</td>
        <td>225</td>
    </tr>
    <tr>
        <td>428</td>
        <td>Dorchester Ave &amp; 63rd St</td>
        <td>26</td>
        <td>26</td>
    </tr>
    <tr>
        <td>270</td>
        <td>Stony Island Ave &amp; 75th St</td>
        <td>5</td>
        <td>11</td>
    </tr>
    <tr>
        <td>367</td>
        <td>Racine Ave &amp; 35th St</td>
        <td>40</td>
        <td>33</td>
    </tr>
    <tr>
        <td>400</td>
        <td>Cottage Grove Ave &amp; 71st St</td>
        <td>6</td>
        <td>6</td>
    </tr>
    <tr>
        <td>639</td>
        <td>Lakefront Trail &amp; Wilson Ave</td>
        <td>19</td>
        <td>27</td>
    </tr>
</table>



## Part 4: Clean up resources

<b><font color='red'>DO NOT RUN THIS UNLESS YOU ARE SURE YOU WANT TO DELETE THE CLUSTER<br/> <b/>
    


```python
#### CAREFUL!!
#-- Uncomment & run to delete the created resources
redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
#### CAREFUL!!
```




    {'Cluster': {'ClusterIdentifier': 'dwhcluster',
      'NodeType': 'dc2.large',
      'ClusterStatus': 'deleting',
      'ClusterAvailabilityStatus': 'Modifying',
      'MasterUsername': 'dwhuser',
      'DBName': 'dwh',
      'Endpoint': {'Address': 'dwhcluster.c4m4dhrmsdov.us-west-2.redshift.amazonaws.com',
       'Port': 5439},
      'ClusterCreateTime': datetime.datetime(2019, 9, 18, 22, 6, 16, 104000, tzinfo=tzutc()),
      'AutomatedSnapshotRetentionPeriod': 1,
      'ManualSnapshotRetentionPeriod': -1,
      'ClusterSecurityGroups': [],
      'VpcSecurityGroups': [{'VpcSecurityGroupId': 'sg-b27b30f3',
        'Status': 'active'}],
      'ClusterParameterGroups': [{'ParameterGroupName': 'default.redshift-1.0',
        'ParameterApplyStatus': 'in-sync'}],
      'ClusterSubnetGroupName': 'default',
      'VpcId': 'vpc-9a53d7e2',
      'AvailabilityZone': 'us-west-2c',
      'PreferredMaintenanceWindow': 'fri:10:30-fri:11:00',
      'PendingModifiedValues': {},
      'ClusterVersion': '1.0',
      'AllowVersionUpgrade': True,
      'NumberOfNodes': 4,
      'PubliclyAccessible': True,
      'Encrypted': False,
      'Tags': [],
      'EnhancedVpcRouting': False,
      'IamRoles': [{'IamRoleArn': 'arn:aws:iam::506140549518:role/dwhRole',
        'ApplyStatus': 'in-sync'}],
      'MaintenanceTrackName': 'current',
      'DeferredMaintenanceWindows': []},
     'ResponseMetadata': {'RequestId': '48535592-da8f-11e9-ab0e-4d85d7c1dfce',
      'HTTPStatusCode': 200,
      'HTTPHeaders': {'x-amzn-requestid': '48535592-da8f-11e9-ab0e-4d85d7c1dfce',
       'content-type': 'text/xml',
       'content-length': '2290',
       'vary': 'Accept-Encoding',
       'date': 'Thu, 19 Sep 2019 03:40:53 GMT'},
      'RetryAttempts': 0}}



- run this block several times until the cluster really deleted


```python
myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
prettyRedshiftProps(myClusterProps)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Key</th>
      <th>Value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ClusterIdentifier</td>
      <td>dwhcluster</td>
    </tr>
    <tr>
      <th>1</th>
      <td>NodeType</td>
      <td>dc2.large</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ClusterStatus</td>
      <td>deleting</td>
    </tr>
    <tr>
      <th>3</th>
      <td>MasterUsername</td>
      <td>dwhuser</td>
    </tr>
    <tr>
      <th>4</th>
      <td>DBName</td>
      <td>dwh</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Endpoint</td>
      <td>{'Address': 'dwhcluster.c4m4dhrmsdov.us-west-2.redshift.amazonaws.com', 'Port': 5439}</td>
    </tr>
    <tr>
      <th>6</th>
      <td>VpcId</td>
      <td>vpc-9a53d7e2</td>
    </tr>
    <tr>
      <th>7</th>
      <td>NumberOfNodes</td>
      <td>4</td>
    </tr>
  </tbody>
</table>
</div>




```python
#### CAREFUL!!
#-- Uncomment & run to delete the created resources
iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
#### CAREFUL!!
```




    {'ResponseMetadata': {'RequestId': '4dd601bf-da8f-11e9-8139-b7143e572bc9',
      'HTTPStatusCode': 200,
      'HTTPHeaders': {'x-amzn-requestid': '4dd601bf-da8f-11e9-8139-b7143e572bc9',
       'content-type': 'text/xml',
       'content-length': '200',
       'date': 'Thu, 19 Sep 2019 03:41:03 GMT'},
      'RetryAttempts': 0}}



## Conclusion
The power of infrastructure-as-code is evident in the fact that we were able to launch a 4-node Redshift cluster, perform our analysis, and destroy all the resources, without once having to login to AWS console. This is the essense of cloud computing, wherein, you can spin the resources as-and-when you want, do whatever task you wish to do, and clean up the resources.
