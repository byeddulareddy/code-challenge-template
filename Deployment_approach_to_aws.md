Extra Credit - Deployment

-------------------------

(Optional.) Assume you are asked to get your code running in the cloud using AWS. What tools and AWS services would you use to deploy the API, database, and a scheduled version of your data ingestion code? Write up a description of your approach.


Solution:

We can do deployments in choosing different servoces and tools, Below approach is my approach.

AWS Services:
1) Lambda
2) EMR
3) AWS RDS Postgress aurora
4) S3
5) IAM
6) Lakeformation
7) EventBridge (Scheduling events from cloudwatch alarms )

Step1: Ingestion
    1) Assume the text files (some weather and crop yield data) or RDBMS or any On-prem is source.
    2) We need to ingest the data from on-prem to AWS S3 using below services
        a) AWS EMR to connect to source RDBMS and using jdbc connector with spark and save to s3
        or
        b) EC2 instance load flat files from sftp location to s3.

Step 2:
    1) I prefer start reading files using lambda function , Because it is serverless and it is cheap when compare with using EC2 and EMR etc.. Assuming if its not meet below limitations
        The disk space (ephemeral) is limited to 512 MB.
        The default deployment package size is 50 MB.
        The memory range is from 128 to 3008 MB.
        The maximum execution timeout for a function is 15 minutes*.
        Requests limitations by Lambda:
        Request and response (synchronous calls) body payload size can be up to 6 MB.
        The event request (asynchronous calls) body can be up to 128 KB.
    2) Using psycopg2 or sqlalchemy(preferred with pandas or spark dataframes) packages and running load_weather_data code in lambda directly loads to postgress table, but we need to setpup below to lambda
        1) Subnects and subnet group id to allow trafic from postgress and lambda
        2) Enable cloudwatch logs to capture the logs.

Deploying Flask in AWS EC2

Third party tool:
1) Concourse
2) Terraform 
3) Swagger UI

AWS Services:
1) AWS EC2
2) AWS API Gateway
3) AWS Lambda
4) AWS RDS Postgress Instance or DynamoDB (Creating database table0s)
5) AWS CloudWatch
6) AWS Secrect Manager to store RDS postgress username,password..connections details etc..
7) AWS Route 53
8) AWS Elastic Beanstalk

Step1 : Using Terreform creating all above aws services 
        AWS Lambda
        1.a.Create lambda fucntion with needed s3 path for python file and layers for needed python packages(psycopg2)
        API Gateway:
        1.b.Create API Gateway and provide swagger template
        1.c.Create API Gateway stage 
        1.d.Create API deployment
        1.e. Create api gateways responce for different responce

        Swagger template:
        1.f.Create template file using swagger_template.json 

        AWS RDS Cluster
        1.g. Create AWS RDS cluster 

        AWS Route53 
        1.h. Create REST API Route53 for domain name

Step2 : Setup concourse pipeline to deploy terraform code 
        2.a. Verify plan
        2.b. Apply changes


Flow:

1)Create Custom domain name in API gateway for swagger
2)From swagger we invoke API gateway with lambda function and payload to pass to lambda
2)API gateways triggers endpount which lambda fucntions and return json object




