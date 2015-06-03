# dynamodb_scaler
Automatically increase dynamoDB table throughputs based on configuration for each table.  Works in concert with the dynamo_table_lister.

This should be run on the same docker hosts as the table_lister container and the volume from that container be mounted in here using the --volumes-from parameter to docker run.

It does NOT lower throughput on a table ever.
