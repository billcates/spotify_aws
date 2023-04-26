# spotify_aws
 spotify_aws
 Data Engineering Project in AWS
 ![architecture](https://user-images.githubusercontent.com/56115142/234548836-a83daacc-5642-47ea-bc23-3c3326382e2f.jpg)

A simple Data Engineering Project which extracts data from spotify api, writes to a csv file, uploads the file to s3 and an event is triggered whenever a new file is ingested into the specified path ,which has the lambda which reads the csv file and uploads to a dynamodb table.

The dynamodb can be used as a target for any api request.
