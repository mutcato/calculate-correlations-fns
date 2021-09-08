bucket_name = calculate-correlations-fns
lambda_name = calculate-correlations-fns

create-bucket:
	aws s3 mb s3://$(bucket_name)

initial-deploy:
	# Creates a zip file considering .gitignore with last commited changes
	git archive HEAD -o lambda_deploy.zip 
	
	# Get in packages directory and add packages into zip file. Then changes current directory
	cd venv/lib/python3.8/site-packages/ && zip -ur ../../../../lambda_deploy.zip . -x "boto*" -x "black*" -x "pip*" -x "__*__" && cd -
	
	# Uploads the zip file into S3 bucket
	aws s3 cp lambda_deploy.zip s3://$(bucket_name)
	
	# Creates a lambda function
	aws lambda create-function \
	--function-name $(lambda_name)  \
	--region eu-west-1 \
	--runtime python3.8  \
	--timeout 600 \
	--memory-size 512 \
	--role ${LAMBDA_IAM_ROLE_ARN} \
	--handler dynamo.lambda_handler \
	--code S3Bucket=$(bucket_name),S3Key=lambda_deploy.zip \
	# If you use --code flag do not use --zip-file flag
	# --zip-file fileb://$(shell pwd)/lambda_deploy.zip 

deploy:
	# Creates a zip file considering .gitignore with last commited changes
	git archive HEAD -o lambda_deploy.zip 

	# Get in packages directory and add packages into zip file. Then changes current directory
	cd venv/lib/python3.8/site-packages/ && zip -ur ../../../../lambda_deploy.zip . -x "boto*" -x "black*" -x "pip*" -x "__*__" && cd -

	# Remove existing s3 file
	aws s3 rm s3://$(bucket_name)/lambda_deploy.zip

	# Uploads the zip file into S3 bucket
	aws s3 mv lambda_deploy.zip s3://$(bucket_name)

	aws lambda update-function-code \
	--function-name $(lambda_name) \
	--s3-bucket $(bucket_name) \
	--s3-key lambda_deploy.zip

run:
	aws lambda invoke --function-name $(lambda_name) outputfile.txt
