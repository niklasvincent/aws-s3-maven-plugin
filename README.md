aws-s3-download-maven-plugin
============================
Download content from S3.

Configuration parameters
------------------------

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
|bucketName|The name of the bucket|*yes*| |
|source|The source amazon s3 file key. Empty to download the whole bucket.|*no*| |
|destination|The destination file or destination folder. Directories *MUST* end with */*| *yes*| |
|overwriteExistingFiles| Whether to overwrite existing files| *no*| false |
|accessKey|S3 access key | *yes* | if unspecified, uses the Default Provider |
|secretKey|S3 secret key | *yes* | if unspecified, uses the Default Provider |
|endpoint|Use a different s3 endpoint| *no* | s3.amazonaws.com |
