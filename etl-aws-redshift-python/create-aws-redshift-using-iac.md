
# Creating Redshift Cluster using the AWS python SDK 
## Author: Shravan Kuchkula

## Introduction
In this post, I will show how we can use AWS python SDK `boto3` to create AWS resources (as opposed to creating from console by clicking through many screens) and launch an AWS Redshift Cluster. Later, we will look into reading data from S3 and storing them into Redshift cluster tables and perform some queries. Finally, we will programatically destroy all the resources that we created so that we don't incur any unnecessary costs.

Many topics are covered in this notebook, below is a list of questions that will be answered:

**Part 1:** **Launch an AWS Redshift cluster**

- How to create IAM account and obtain the __AWS_KEY__ and __AWS_SECRET__?
- How to __connect to AWS console__ programatically using the above IAM user?
- How to use __configparser__ to read in a DWH config file with all the variables?
- How to use AWS python SDK using __boto3__ package?
- How to create __S3, EC2, IAM and Redshift__ clients using boto3?
- How to create **IAM role** and assign a policy?
- How to create and launch a **RedShift cluster**?
- How to monitor a **Redshift cluster state**?
- How to create a **security group** for RedShift cluster to access S3?

**Part 2:** **Connect to cluster, load data and perform analysis**

- How to **connect to Redshift cluster from Jupyter Notebook**?
- How to **load a partitioned and non-partitioned data from a S3 bucket** into a table in Redshift?
- How to create tables with **no-distribution** strategy?
- How to create tables with a **distribution** strategy?
- Compare the performance of **nodist vs dist** strategy.


**Part 3:** **Destroy the resources**

- How to **delete** a Redshift cluster?
- How to **delete** IAM role, security groups and other resources that we created in part1?

### About the role of Redshift in the overall data engineering pipeline
AWS Redshift is a cloud-managed, columnar, Massively Parallel Processing (MPP) data warehouse service in the cloud.

Shown below is the role of AWS Redshift in the big picture of a typical data engineering pipeline. The sources can be a diverse mix of databases (Cassandra, DynamoDB, MySQL, Postgres), EC2 machines, other S3 locations. Then, there is this class of products which are responsible for doing the actual ETL calls. The goal of this suite of products is to issue calls to the sources, extract the data from them, do some transformations and then store the transformed data into S3 - which is our defacto staging area. Some products in this category, which are commonly referred to as *dataflow products* are: **Airflow, Luigi, Nifi, StreamSet or AWS Data pipeline**.


In this post, my focus is on the highlighted box, where we are assuming there are some csv files already loaded onto the S3 staging area by some ETL process. Our goal is to ingest this csv data from S3 into Amazon Redshift cluster. 

![](redshift-project2.png)

Once the data is in Redshift, our chief goal is for the BI apps to be able to connect to Redshift cluster and do some analysis. The BI apps can either directly connect to the Redshift cluster or go through an intermediate stage where data is in the form of aggregations represented by OLAP cubes. 

Here, I will be making use of this jupyter notebook to make a direct connection to the Redshift cluster and create tables, load data from S3 into Redshift cluster, and perform some analysis. In the real-world, there could be full-fledged BI products which can feed from the Redshift cluster and do some sophisticated analysis.

Finally, I will illustrate the power of Infrastructure-as-code, through which we can provision, use and destroy AWS resources very rapidly, and in a reproducible manner.

## Part 1: Launch an AWS Redshift Cluster

### Step 1: Create an IAM user
In most cases, people are given an IAM user to work with. If you are doing this from scratch, then go into your AWS console and create a new IAM user.

- Create a new IAM user in your AWS account
- Give it `AdministratorAccess`, From `Attach existing policies directly` Tab
- Take note of the access key and secret 
- Edit the file `dwh.cfg` in the same folder as this notebook and fill
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
config.read_file(open('dwh.cfg'))

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
Here, we will be using `boto3` to get the resource handles to talk to S3, EC2, IAM and Redshift. Notice that for Redshift, you will be creating a client. I am choosing `us-east-1` as my region.


```python
import boto3

ec2 = boto3.resource('ec2',
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )

s3 = boto3.resource('s3',
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )

iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-east-1'
                  )

redshift = boto3.client('redshift',
                       region_name="us-east-1",
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
      <td>{'Address': 'dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com', 'Port': 5439}</td>
    </tr>
    <tr>
      <th>6</th>
      <td>VpcId</td>
      <td>vpc-ade05fd7</td>
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

    DWN_ENDPOINT ::  dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com
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

    ec2.SecurityGroup(id='sg-0a945e1c987ca77dd')


## Part 2: Connect to cluster, load data and perform analysis

In the second part, we will be making use of the cluster we just launched in part 1. 

Using sql extension, we can directly run SQL commands within jupyter notebook. 
- A single `%sql` means the query is a python string accessed using the dollar sign.
- A `%%sql` means the query is not a python string but can be multiline SQL statements 



### Step 1: Load sql extension and connect to the cluster


```python
%load_ext sql
```


```python
conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
print(conn_string)
%sql $conn_string
```

    postgresql://dwhuser:Passw0rd@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh





    'Connected: dwhuser@dwh'



### Step 2: Get some data from S3 
I will be using the `udacity-labs` S3 bucket which is in `us-west-2` region to retrieve the tickets data. Notice, that we have two version of this dataset available: one is partitioned into parts, second is full.csv.gz. 


```python
s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                     )

sampleDbBucket =  s3.Bucket("udacity-labs")

for obj in sampleDbBucket.objects.filter(Prefix="tickets"):
    print(obj)
```

    s3.ObjectSummary(bucket_name='udacity-labs', key='tickets/')
    s3.ObjectSummary(bucket_name='udacity-labs', key='tickets/full/')
    s3.ObjectSummary(bucket_name='udacity-labs', key='tickets/full/full.csv.gz')
    s3.ObjectSummary(bucket_name='udacity-labs', key='tickets/split/')
    s3.ObjectSummary(bucket_name='udacity-labs', key='tickets/split/part-00000-d33afb94-b8af-407d-abd5-59c0ee8f5ee8-c000.csv.gz')
    s3.ObjectSummary(bucket_name='udacity-labs', key='tickets/split/part-00001-d33afb94-b8af-407d-abd5-59c0ee8f5ee8-c000.csv.gz')
    s3.ObjectSummary(bucket_name='udacity-labs', key='tickets/split/part-00002-d33afb94-b8af-407d-abd5-59c0ee8f5ee8-c000.csv.gz')
    s3.ObjectSummary(bucket_name='udacity-labs', key='tickets/split/part-00003-d33afb94-b8af-407d-abd5-59c0ee8f5ee8-c000.csv.gz')
    s3.ObjectSummary(bucket_name='udacity-labs', key='tickets/split/part-00004-d33afb94-b8af-407d-abd5-59c0ee8f5ee8-c000.csv.gz')
    s3.ObjectSummary(bucket_name='udacity-labs', key='tickets/split/part-00005-d33afb94-b8af-407d-abd5-59c0ee8f5ee8-c000.csv.gz')
    s3.ObjectSummary(bucket_name='udacity-labs', key='tickets/split/part-00006-d33afb94-b8af-407d-abd5-59c0ee8f5ee8-c000.csv.gz')
    s3.ObjectSummary(bucket_name='udacity-labs', key='tickets/split/part-00007-d33afb94-b8af-407d-abd5-59c0ee8f5ee8-c000.csv.gz')
    s3.ObjectSummary(bucket_name='udacity-labs', key='tickets/split/part-00008-d33afb94-b8af-407d-abd5-59c0ee8f5ee8-c000.csv.gz')
    s3.ObjectSummary(bucket_name='udacity-labs', key='tickets/split/part-00009-d33afb94-b8af-407d-abd5-59c0ee8f5ee8-c000.csv.gz')


### Step 3: Create tables for partitioned data


```python
%%sql 
DROP TABLE IF EXISTS "sporting_event_ticket";
CREATE TABLE "sporting_event_ticket" (
    "id" double precision DEFAULT nextval('sporting_event_ticket_seq') NOT NULL,
    "sporting_event_id" double precision NOT NULL,
    "sport_location_id" double precision NOT NULL,
    "seat_level" numeric(1,0) NOT NULL,
    "seat_section" character varying(15) NOT NULL,
    "seat_row" character varying(10) NOT NULL,
    "seat" character varying(10) NOT NULL,
    "ticketholder_id" double precision,
    "ticket_price" numeric(8,2) NOT NULL
);
```

     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    Done.





    []



### Step 4: Load partitioned data from S3 into the table just created
When moving large amounts of data from S3 staging area to Redshift, it is better to use the `copy` command instead of `insert`. The benefit of using the copy command is that the ingestion can be parallelized if the data is broken into parts. Each part can be independently ingested by a slice in the cluster. As we are creating 4 nodes in this cluster, we can assume that there will be 4 parallel ingestions into the cluster. This can significantly reduce the time it takes to ingest large payloads.


```python
%%time
qry = """
    copy sporting_event_ticket from 's3://udacity-labs/tickets/split/part'
    credentials 'aws_iam_role={}'
    gzip delimiter ';' compupdate off region 'us-west-2';
""".format(DWH_ROLE_ARN)

%sql $qry
```

     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    CPU times: user 3.82 ms, sys: 2.51 ms, total: 6.34 ms
    Wall time: 12.6 s


### Step 5: Create tables for non-partioned data
To hammer the point home, that if we have non-partitioned data, which cannot be ingested in parallel, it takes more time, even when you are using copy. Since copy command can only parallelize a job, if the data is partitioned. 

Notice the wall time is almost double.


```python
%%sql
DROP TABLE IF EXISTS "sporting_event_ticket_full";
CREATE TABLE "sporting_event_ticket_full" (
    "id" double precision DEFAULT nextval('sporting_event_ticket_seq') NOT NULL,
    "sporting_event_id" double precision NOT NULL,
    "sport_location_id" double precision NOT NULL,
    "seat_level" numeric(1,0) NOT NULL,
    "seat_section" character varying(15) NOT NULL,
    "seat_row" character varying(10) NOT NULL,
    "seat" character varying(10) NOT NULL,
    "ticketholder_id" double precision,
    "ticket_price" numeric(8,2) NOT NULL
);
```

     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    Done.





    []




```python
%%time

qry = """
    copy sporting_event_ticket_full from 's3://udacity-labs/tickets/full/full.csv.gz' 
    credentials 'aws_iam_role={}' 
    gzip delimiter ';' compupdate off region 'us-west-2';
""".format(DWH_ROLE_ARN)

%sql $qry
```

     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    CPU times: user 4.1 ms, sys: 2.59 ms, total: 6.69 ms
    Wall time: 24.6 s


### Step 6: Create tables with no distribution strategy

**What is the need for distribution strategy?**: When a table is partitioned up into many pieces and distributed across slices in different machines, this partitioning is done blindly, that is, we don't make use of prior knowledge of the frequent access patterns of a table.  If one has an idea about the frequent access pattern of a table, one can choose a more clever strategy. There are two possible strategies:
- Distribution Style
- Sorting Key

We will use the lineorder dataset which is common for benchmarking start schemas in data warehouses. The data is pre-loaded in a public bucket on the us-west-2 region.
![lineorder](lineorder.png)



```python
%%sql 
CREATE SCHEMA IF NOT EXISTS nodist;
SET search_path TO nodist;

DROP TABLE IF EXISTS part cascade;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS dwdate;
DROP TABLE IF EXISTS lineorder;

CREATE TABLE part 
(
  p_partkey     INTEGER NOT NULL,
  p_name        VARCHAR(22) NOT NULL,
  p_mfgr        VARCHAR(6) NOT NULL,
  p_category    VARCHAR(7) NOT NULL,
  p_brand1      VARCHAR(9) NOT NULL,
  p_color       VARCHAR(11) NOT NULL,
  p_type        VARCHAR(25) NOT NULL,
  p_size        INTEGER NOT NULL,
  p_container   VARCHAR(10) NOT NULL
);

CREATE TABLE supplier 
(
  s_suppkey   INTEGER NOT NULL,
  s_name      VARCHAR(25) NOT NULL,
  s_address   VARCHAR(25) NOT NULL,
  s_city      VARCHAR(10) NOT NULL,
  s_nation    VARCHAR(15) NOT NULL,
  s_region    VARCHAR(12) NOT NULL,
  s_phone     VARCHAR(15) NOT NULL
);

CREATE TABLE customer 
(
  c_custkey      INTEGER NOT NULL,
  c_name         VARCHAR(25) NOT NULL,
  c_address      VARCHAR(25) NOT NULL,
  c_city         VARCHAR(10) NOT NULL,
  c_nation       VARCHAR(15) NOT NULL,
  c_region       VARCHAR(12) NOT NULL,
  c_phone        VARCHAR(15) NOT NULL,
  c_mktsegment   VARCHAR(10) NOT NULL
);

CREATE TABLE dwdate 
(
  d_datekey            INTEGER NOT NULL,
  d_date               VARCHAR(19) NOT NULL,
  d_dayofweek          VARCHAR(10) NOT NULL,
  d_month              VARCHAR(10) NOT NULL,
  d_year               INTEGER NOT NULL,
  d_yearmonthnum       INTEGER NOT NULL,
  d_yearmonth          VARCHAR(8) NOT NULL,
  d_daynuminweek       INTEGER NOT NULL,
  d_daynuminmonth      INTEGER NOT NULL,
  d_daynuminyear       INTEGER NOT NULL,
  d_monthnuminyear     INTEGER NOT NULL,
  d_weeknuminyear      INTEGER NOT NULL,
  d_sellingseason      VARCHAR(13) NOT NULL,
  d_lastdayinweekfl    VARCHAR(1) NOT NULL,
  d_lastdayinmonthfl   VARCHAR(1) NOT NULL,
  d_holidayfl          VARCHAR(1) NOT NULL,
  d_weekdayfl          VARCHAR(1) NOT NULL
);
CREATE TABLE lineorder 
(
  lo_orderkey          INTEGER NOT NULL,
  lo_linenumber        INTEGER NOT NULL,
  lo_custkey           INTEGER NOT NULL,
  lo_partkey           INTEGER NOT NULL,
  lo_suppkey           INTEGER NOT NULL,
  lo_orderdate         INTEGER NOT NULL,
  lo_orderpriority     VARCHAR(15) NOT NULL,
  lo_shippriority      VARCHAR(1) NOT NULL,
  lo_quantity          INTEGER NOT NULL,
  lo_extendedprice     INTEGER NOT NULL,
  lo_ordertotalprice   INTEGER NOT NULL,
  lo_discount          INTEGER NOT NULL,
  lo_revenue           INTEGER NOT NULL,
  lo_supplycost        INTEGER NOT NULL,
  lo_tax               INTEGER NOT NULL,
  lo_commitdate        INTEGER NOT NULL,
  lo_shipmode          VARCHAR(10) NOT NULL
);
```

     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    Done.
    Done.
    Done.
    Done.
    Done.
    Done.
    Done.
    Done.
    Done.
    Done.
    Done.
    Done.





    []



### Step 7: Create tables with distribution strategy


```python
%%sql

CREATE SCHEMA IF NOT EXISTS dist;
SET search_path TO dist;

DROP TABLE IF EXISTS part cascade;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS dwdate;
DROP TABLE IF EXISTS lineorder;

CREATE TABLE part (
  p_partkey     	integer     	not null	sortkey distkey,
  p_name        	varchar(22) 	not null,
  p_mfgr        	varchar(6)      not null,
  p_category    	varchar(7)      not null,
  p_brand1      	varchar(9)      not null,
  p_color       	varchar(11) 	not null,
  p_type        	varchar(25) 	not null,
  p_size        	integer     	not null,
  p_container   	varchar(10)     not null
);

CREATE TABLE supplier (
  s_suppkey     	integer        not null sortkey,
  s_name        	varchar(25)    not null,
  s_address     	varchar(25)    not null,
  s_city        	varchar(10)    not null,
  s_nation      	varchar(15)    not null,
  s_region      	varchar(12)    not null,
  s_phone       	varchar(15)    not null)
diststyle all;

CREATE TABLE customer (
  c_custkey     	integer        not null sortkey,
  c_name        	varchar(25)    not null,
  c_address     	varchar(25)    not null,
  c_city        	varchar(10)    not null,
  c_nation      	varchar(15)    not null,
  c_region      	varchar(12)    not null,
  c_phone       	varchar(15)    not null,
  c_mktsegment      varchar(10)    not null)
diststyle all;

CREATE TABLE dwdate (
  d_datekey            integer       not null sortkey,
  d_date               varchar(19)   not null,
  d_dayofweek	      varchar(10)   not null,
  d_month      	    varchar(10)   not null,
  d_year               integer       not null,
  d_yearmonthnum       integer  	 not null,
  d_yearmonth          varchar(8)	not null,
  d_daynuminweek       integer       not null,
  d_daynuminmonth      integer       not null,
  d_daynuminyear       integer       not null,
  d_monthnuminyear     integer       not null,
  d_weeknuminyear      integer       not null,
  d_sellingseason      varchar(13)    not null,
  d_lastdayinweekfl    varchar(1)    not null,
  d_lastdayinmonthfl   varchar(1)    not null,
  d_holidayfl          varchar(1)    not null,
  d_weekdayfl          varchar(1)    not null)
diststyle all;

CREATE TABLE lineorder (
  lo_orderkey      	    integer     	not null,
  lo_linenumber        	integer     	not null,
  lo_custkey           	integer     	not null,
  lo_partkey           	integer     	not null distkey,
  lo_suppkey           	integer     	not null,
  lo_orderdate         	integer     	not null sortkey,
  lo_orderpriority     	varchar(15)     not null,
  lo_shippriority      	varchar(1)      not null,
  lo_quantity          	integer     	not null,
  lo_extendedprice     	integer     	not null,
  lo_ordertotalprice   	integer     	not null,
  lo_discount          	integer     	not null,
  lo_revenue           	integer     	not null,
  lo_supplycost        	integer     	not null,
  lo_tax               	integer     	not null,
  lo_commitdate         integer         not null,
  lo_shipmode          	varchar(10)     not null
);
```

     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    Done.
    Done.
    Done.
    Done.
    Done.
    Done.
    Done.
    Done.
    Done.
    Done.
    Done.
    Done.





    []



### Step 8: Load the data from S3 into dist and no-dist tables we created above
Our intent here is to run 5 COPY operations for the 5 tables respectively as show below.

However, we want to do accomplish the following:
- Make sure that the `DWH_ROLE_ARN` is substituted with the correct value in each query
- Perform the data loading twice once for each schema (dist and nodist)
- Collect timing statistics to compare the insertion times
Thus, we have scripted the insertion as found below in the function `loadTables` which
returns a pandas dataframe containing timing statistics for the copy operations

```sql
copy customer from 's3://awssampledbuswest2/ssbgz/customer' 
credentials 'aws_iam_role=<DWH_ROLE_ARN>'
gzip region 'us-west-2';

copy dwdate from 's3://awssampledbuswest2/ssbgz/dwdate' 
credentials 'aws_iam_role=<DWH_ROLE_ARN>'
gzip region 'us-west-2';

copy lineorder from 's3://awssampledbuswest2/ssbgz/lineorder' 
credentials 'aws_iam_role=<DWH_ROLE_ARN>'
gzip region 'us-west-2';

copy part from 's3://awssampledbuswest2/ssbgz/part' 
credentials 'aws_iam_role=<DWH_ROLE_ARN>'
gzip region 'us-west-2';

copy supplier from 's3://awssampledbuswest2/ssbgz/supplier' 
credentials 'aws_iam_role=<DWH_ROLE_ARN>'
gzip region 'us-west-2';
```


```python
import os
from time import time
```


```python
def loadTables(schema, tables):
    loadTimes = []
    SQL_SET_SCEMA = "SET search_path TO {};".format(schema)
    %sql $SQL_SET_SCEMA
    
    for table in tables:
        SQL_COPY = """
copy {} from 's3://awssampledbuswest2/ssbgz/{}' 
credentials 'aws_iam_role={}'
gzip region 'us-west-2';
        """.format(table,table, DWH_ROLE_ARN)

        print("======= LOADING TABLE: ** {} ** IN SCHEMA ==> {} =======".format(table, schema))
        print(SQL_COPY)

        t0 = time()
        %sql $SQL_COPY
        loadTime = time()-t0
        loadTimes.append(loadTime)

        print("=== DONE IN: {0:.2f} sec\n".format(loadTime))
    return pd.DataFrame({"table":tables, "loadtime_"+schema:loadTimes}).set_index('table')
```


```python
#-- List of the tables to be loaded
tables = ["customer","dwdate","supplier", "part", "lineorder"]

#-- Insertion twice for each schema (WARNING!! EACH CAN TAKE MORE THAN 10 MINUTES!!!)
nodistStats = loadTables("nodist", tables)
distStats = loadTables("dist", tables)
```

     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    ======= LOADING TABLE: ** customer ** IN SCHEMA ==> nodist =======
    
    copy customer from 's3://awssampledbuswest2/ssbgz/customer' 
    credentials 'aws_iam_role=arn:aws:iam::506140549518:role/dwhRole'
    gzip region 'us-west-2';
            
     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    === DONE IN: 27.38 sec
    
    ======= LOADING TABLE: ** dwdate ** IN SCHEMA ==> nodist =======
    
    copy dwdate from 's3://awssampledbuswest2/ssbgz/dwdate' 
    credentials 'aws_iam_role=arn:aws:iam::506140549518:role/dwhRole'
    gzip region 'us-west-2';
            
     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    === DONE IN: 14.44 sec
    
    ======= LOADING TABLE: ** supplier ** IN SCHEMA ==> nodist =======
    
    copy supplier from 's3://awssampledbuswest2/ssbgz/supplier' 
    credentials 'aws_iam_role=arn:aws:iam::506140549518:role/dwhRole'
    gzip region 'us-west-2';
            
     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    === DONE IN: 21.40 sec
    
    ======= LOADING TABLE: ** part ** IN SCHEMA ==> nodist =======
    
    copy part from 's3://awssampledbuswest2/ssbgz/part' 
    credentials 'aws_iam_role=arn:aws:iam::506140549518:role/dwhRole'
    gzip region 'us-west-2';
            
     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    === DONE IN: 17.51 sec
    
    ======= LOADING TABLE: ** lineorder ** IN SCHEMA ==> nodist =======
    
    copy lineorder from 's3://awssampledbuswest2/ssbgz/lineorder' 
    credentials 'aws_iam_role=arn:aws:iam::506140549518:role/dwhRole'
    gzip region 'us-west-2';
            
     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    === DONE IN: 558.49 sec
    
     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    ======= LOADING TABLE: ** customer ** IN SCHEMA ==> dist =======
    
    copy customer from 's3://awssampledbuswest2/ssbgz/customer' 
    credentials 'aws_iam_role=arn:aws:iam::506140549518:role/dwhRole'
    gzip region 'us-west-2';
            
     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    === DONE IN: 27.10 sec
    
    ======= LOADING TABLE: ** dwdate ** IN SCHEMA ==> dist =======
    
    copy dwdate from 's3://awssampledbuswest2/ssbgz/dwdate' 
    credentials 'aws_iam_role=arn:aws:iam::506140549518:role/dwhRole'
    gzip region 'us-west-2';
            
     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    === DONE IN: 7.93 sec
    
    ======= LOADING TABLE: ** supplier ** IN SCHEMA ==> dist =======
    
    copy supplier from 's3://awssampledbuswest2/ssbgz/supplier' 
    credentials 'aws_iam_role=arn:aws:iam::506140549518:role/dwhRole'
    gzip region 'us-west-2';
            
     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    === DONE IN: 18.90 sec
    
    ======= LOADING TABLE: ** part ** IN SCHEMA ==> dist =======
    
    copy part from 's3://awssampledbuswest2/ssbgz/part' 
    credentials 'aws_iam_role=arn:aws:iam::506140549518:role/dwhRole'
    gzip region 'us-west-2';
            
     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    === DONE IN: 16.38 sec
    
    ======= LOADING TABLE: ** lineorder ** IN SCHEMA ==> dist =======
    
    copy lineorder from 's3://awssampledbuswest2/ssbgz/lineorder' 
    credentials 'aws_iam_role=arn:aws:iam::506140549518:role/dwhRole'
    gzip region 'us-west-2';
            
     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    === DONE IN: 712.86 sec
    


### Step 9: Compare load times with and without dist strategy


```python
import matplotlib.pyplot as plt
import pandas as pd
```


```python
#-- Plotting of the timing results
stats = distStats.join(nodistStats)
stats.plot.bar()
plt.show()
```


![png](create-aws-redshift-using-iac_files/create-aws-redshift-using-iac_40_0.png)


### Step 10: Compare query performance with and without dist strategy


```python
oneDim_SQL ="""
set enable_result_cache_for_session to off;
SET search_path TO {};

select sum(lo_extendedprice*lo_discount) as revenue
from lineorder, dwdate
where lo_orderdate = d_datekey
and d_year = 1997 
and lo_discount between 1 and 3 
and lo_quantity < 24;
"""

twoDim_SQL="""
set enable_result_cache_for_session to off;
SET search_path TO {};

select sum(lo_revenue), d_year, p_brand1
from lineorder, dwdate, part, supplier
where lo_orderdate = d_datekey
and lo_partkey = p_partkey
and lo_suppkey = s_suppkey
and p_category = 'MFGR#12'
and s_region = 'AMERICA'
group by d_year, p_brand1
"""

drill_SQL = """
set enable_result_cache_for_session to off;
SET search_path TO {};

select c_city, s_city, d_year, sum(lo_revenue) as revenue 
from customer, lineorder, supplier, dwdate
where lo_custkey = c_custkey
and lo_suppkey = s_suppkey
and lo_orderdate = d_datekey
and (c_city='UNITED KI1' or
c_city='UNITED KI5')
and (s_city='UNITED KI1' or
s_city='UNITED KI5')
and d_yearmonth = 'Dec1997'
group by c_city, s_city, d_year
order by d_year asc, revenue desc;
"""


oneDimSameDist_SQL ="""
set enable_result_cache_for_session to off;
SET search_path TO {};

select lo_orderdate, sum(lo_extendedprice*lo_discount) as revenue  
from lineorder, part
where lo_partkey  = p_partkey
group by lo_orderdate
order by lo_orderdate
"""

def compareQueryTimes(schema):
    queryTimes  =[] 
    for i,query in enumerate([oneDim_SQL, twoDim_SQL, drill_SQL, oneDimSameDist_SQL]):
        t0 = time()
        q = query.format(schema)
        %sql $q
        queryTime = time()-t0
        queryTimes.append(queryTime)
    return pd.DataFrame({"query":["oneDim","twoDim", "drill", "oneDimSameDist"], "queryTime_"+schema:queryTimes}).set_index('query')
```


```python
noDistQueryTimes = compareQueryTimes("nodist")
distQueryTimes   = compareQueryTimes("dist") 
```

     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    Done.
    1 rows affected.
     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    Done.
    280 rows affected.
     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    Done.
    4 rows affected.
     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    Done.
    2406 rows affected.
     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    Done.
    1 rows affected.
     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    Done.
    280 rows affected.
     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    Done.
    4 rows affected.
     * postgresql://dwhuser:***@dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com:5439/dwh
    Done.
    Done.
    2406 rows affected.



```python
queryTimeDF =noDistQueryTimes.join(distQueryTimes)
queryTimeDF.plot.bar()
plt.show()
```


![png](create-aws-redshift-using-iac_files/create-aws-redshift-using-iac_44_0.png)



```python
improvementDF = queryTimeDF["distImprovement"] =100.0*(queryTimeDF['queryTime_nodist']-queryTimeDF['queryTime_dist'])/queryTimeDF['queryTime_nodist']
improvementDF.plot.bar(title="% dist Improvement by query")
plt.show()
```


![png](create-aws-redshift-using-iac_files/create-aws-redshift-using-iac_45_0.png)


## Part 3: Clean up resources

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
      'MasterUsername': 'dwhuser',
      'DBName': 'dwh',
      'Endpoint': {'Address': 'dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com',
       'Port': 5439},
      'ClusterCreateTime': datetime.datetime(2019, 8, 30, 20, 33, 18, 333000, tzinfo=tzutc()),
      'AutomatedSnapshotRetentionPeriod': 1,
      'ManualSnapshotRetentionPeriod': -1,
      'ClusterSecurityGroups': [],
      'VpcSecurityGroups': [{'VpcSecurityGroupId': 'sg-1df7ce58',
        'Status': 'active'}],
      'ClusterParameterGroups': [{'ParameterGroupName': 'default.redshift-1.0',
        'ParameterApplyStatus': 'in-sync'}],
      'ClusterSubnetGroupName': 'default',
      'VpcId': 'vpc-ade05fd7',
      'AvailabilityZone': 'us-east-1f',
      'PreferredMaintenanceWindow': 'sat:07:30-sat:08:00',
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
     'ResponseMetadata': {'RequestId': 'a5cf0ad6-cb67-11e9-994b-1737f36bbeac',
      'HTTPStatusCode': 200,
      'HTTPHeaders': {'x-amzn-requestid': 'a5cf0ad6-cb67-11e9-994b-1737f36bbeac',
       'content-type': 'text/xml',
       'content-length': '2290',
       'vary': 'Accept-Encoding',
       'date': 'Fri, 30 Aug 2019 20:49:22 GMT'},
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
      <td>{'Address': 'dwhcluster.cpczrz48gy51.us-east-1.redshift.amazonaws.com', 'Port': 5439}</td>
    </tr>
    <tr>
      <th>6</th>
      <td>VpcId</td>
      <td>vpc-ade05fd7</td>
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




    {'ResponseMetadata': {'RequestId': 'c4739663-cb8c-11e9-addf-4bb7f7ab8387',
      'HTTPStatusCode': 200,
      'HTTPHeaders': {'x-amzn-requestid': 'c4739663-cb8c-11e9-addf-4bb7f7ab8387',
       'content-type': 'text/xml',
       'content-length': '200',
       'date': 'Sat, 31 Aug 2019 01:15:05 GMT'},
      'RetryAttempts': 0}}



## Conclusion
The power of infrastructure-as-code is evident in the fact that we were able to launch a 4-node Redshift cluster, perform our analysis, and destroy all the resources, without once having to login to AWS console. This is the essense of cloud computing, wherein, you can spin the resources as-and-when you want, do whatever task you wish to do, and clean up the resources.
