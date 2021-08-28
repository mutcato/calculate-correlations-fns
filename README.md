# calculate-correlations-fns
Lambda function calculates correlations between close prices

Open an AWS account. Create an IAM user. Download the credentials into .aws folder in your home directory

Create an IAM role with DynamoDB read, write permissions

Export this IAM ARN as an envinment variable:
    export LAMBDA_IAM_ROLE_ARN=yourcreatedlambdaiam

Create an S3 bucket:
    make create-bucket

Create a Lambda function with project files:
    make initial-deploy
