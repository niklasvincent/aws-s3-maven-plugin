package info.lindblad.maven.plugins.aws.s3.download;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.*;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Optional;

@Mojo(name = "aws-s3-download")
public class AWSS3DownloadMojo extends AbstractMojo {

    @Parameter(property = "aws-s3-download.accessKey")
    private String accessKey;

    @Parameter(property = "aws-s3-download.secretKey")
    private String secretKey;

    @Parameter(property = "aws-s3-download.source")
    private String source;

    @Parameter(property = "aws-s3-download.bucketName", required = true)
    private String bucketName;

    @Parameter(property = "aws-s3-download.destination", required = true)
    private String destination;

    @Parameter(property = "aws-s3-download.overwriteExistingFiles")
    private Boolean overwriteExistingFiles;

    @Parameter(property = "aws-s3-download.endpoint")
    private String endpoint;

    @Parameter(property = "aws-s3-download.region")
    private String region;

    @Override
    public void execute() throws MojoExecutionException {
        getLog().info(String.format("Bucket: %s, source: %s, destination: %s", bucketName, source, destination));

        source = Optional.ofNullable(source).orElse("");

        // Do not overwrite existing files by default
        overwriteExistingFiles = (null == overwriteExistingFiles) ? false : overwriteExistingFiles;

        AmazonS3 s3Client = getS3Client(accessKey, secretKey, endpoint, region);

        if (!s3Client.doesBucketExist(bucketName)) {
            throw new MojoExecutionException("No such S3 bucket: " + bucketName);
        }

        try {
            download(s3Client);
        } catch (IOException e) {
            e.printStackTrace();
            throw new MojoExecutionException("Unable to download file(s) from S3.");
        }

        getLog().info("Successfully downloaded all files");
    }

    /**
     * Build and return an AWS S3 client
     *
     * @param accessKey  Access key
     * @param secretKey  Secret key
     * @param endpoint   S3 endpoint
     * @param region     AWS region
     * @return Amazon S3 client
     */
    private static AmazonS3 getS3Client(String accessKey, String secretKey, String endpoint, String region) throws MojoExecutionException {
        AWSCredentialsProvider credentialsProvider;
        if (accessKey != null && secretKey != null) {
            AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
            credentialsProvider = new AWSStaticCredentialsProvider(credentials);
        } else {
            credentialsProvider = new DefaultAWSCredentialsProviderChain();
            try {
                credentialsProvider.getCredentials();
            } catch (SdkClientException e) {
                throw new MojoExecutionException("Could not load AWS credentials");
            }
        }

        AmazonS3ClientBuilder amazonS3ClientBuilder = AmazonS3ClientBuilder.standard();
        amazonS3ClientBuilder.withCredentials(credentialsProvider);

        if (null != endpoint && null != region) {
            AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(
                    endpoint,
                    region
            );
            amazonS3ClientBuilder.withEndpointConfiguration(endpointConfiguration);
        } else if (null != endpoint) {
            throw new MojoExecutionException("Cannot specify endpoint without region");
        }

        if (null != region) {
            amazonS3ClientBuilder.setRegion(region);
        }

        return amazonS3ClientBuilder.build();
    }

    /**
     * Check whether path has a trailing slash or not
     *
     * @param path File path
     * @return Whether it is a directory or not
     */
    private static boolean isDirectory(String path) {
        return path.charAt(path.length() - 1) == '/';
    }

    /**
     * Ensure a directory exists
     *
     * @param file  The directory
     */
    private void mkdirs(File file) {
        boolean directoryCreated = file.mkdirs();
        if (directoryCreated) {
            getLog().info(String.format("Created new directory: %s\n", file.getName()));
        }
    }

    /**
     * Download source file(s) from Amazon S3
     *
     * @param s3Client    Amazon S3 client
     * @throws MojoExecutionException
     */
    private void download(AmazonS3 s3Client) throws MojoExecutionException, IOException {
        File destinationFile = new File(destination);
        if (isDirectory(destination)) {
            mkdirs(destinationFile);

            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                    .withBucketName(bucketName)
                    .withPrefix(source);
            ObjectListing objectListing;

            do {
                objectListing = s3Client.listObjects(listObjectsRequest);
                for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                    download(s3Client, destinationFile, objectSummary.getKey());
                }

                listObjectsRequest.setMarker(objectListing.getNextMarker());
            } while (objectListing.isTruncated());
        } else {
            download(s3Client, destinationFile, source);
        }
    }

    /**
     * Download a single file
     *
     * @param s3Client    Amazon S3 client
     * @param destination Destination path
     * @param key         Amazon S3 key
     * @throws IOException
     */
    private void download(AmazonS3 s3Client, File destination, String key) throws IOException {
        File newDestination = destination.toPath().resolve(key).toFile();

        if (isDirectory(key)) {
            mkdirs(newDestination);
        } else if (newDestination.exists() && !overwriteExistingFiles) {
            getLog().info(String.format("Skipping existing file: %s", newDestination.getName()));
        } else {
            mkdirs(newDestination.getParentFile());

            GetObjectRequest request = new GetObjectRequest(bucketName, key);
            S3Object object = s3Client.getObject(request);
            S3ObjectInputStream objectContent = object.getObjectContent();

            getLog().info(String.format("Overwriting existing file: %s", newDestination.getName()));
            Files.copy(objectContent, newDestination.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }

}
