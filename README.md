# dynamodb_scaler
Automatically increase dynamoDB table throughputs based on configuration for each table.
Works in concert with the dynamo_table_lister.

This should be run on the same docker host(s) as the table_lister container and the volume from that container
be mounted in here using the --volumes-from parameter to docker run.

It does NOT lower throughput on a table ever.

## Config file
The app will read the json config file and process any values in there.  If no keys are specified, instance role
credentials are used.  NOTE that values specified in variables OVERRIDE those specified in the config file

## Variables:

- DYNAMODB_REGION : The dynamoDB endpoint to connect to for monitoring tables
- FREQUENCY: How often to check the table throughput (in seconds).  Default 300
- VERBOSE: Enable more logging information
- TEST_MODE: Set to true to evaluate tables but not change any

## Example Docker run

```
#!/bin/bash

docker run -d -e "DYNAMODB_REGION=dynamodb.us-east-1.amazonaws.com" \
              -e "FREQUENCY=86400" \
              -e "TEST_MODE=true" \
	      --name dynamo-scaler \
	      --volumes-from dynamo-table-lister \
              signiant/dynamodb-scaler
````
## Running under the EC2 Container Service (ECS)

A sample task definition is included under the ecs folder.  This runs both required containers under one ECS task
