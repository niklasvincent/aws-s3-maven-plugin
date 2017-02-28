package info.lindblad.maven.plugins.aws.s3;

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
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;

@Mojo(name = "aws-s3")
public class AWSS3Mojo extends AbstractMojo {

    @Parameter(property = "aws-s3.accessKey")
    private String accessKey;

    @Parameter(property = "aws-s3.secretKey")
    private String secretKey;

    @Parameter(property = "aws-s3.s3Objects")
    private List<info.lindblad.maven.plugins.aws.s3.S3Object> s3Objects;

    @Parameter(property = "aws-s3.endpoint")
    private String endpoint;

    @Parameter(property = "aws-s3.region")
    private String region;

    private AmazonS3 s3Client;

    protected AWSS3Mojo(AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }

    @Override
    public void execute() throws MojoExecutionException {
        getLog().info(String.format("%d objects to consider", s3Objects.size()));

        registerS3ProtocolHandler();

        if (s3Client == null) {
            s3Client = getS3Client(accessKey, secretKey, endpoint, region);
        }

        try {
            process(s3Objects);
        } catch (IOException e) {
            e.printStackTrace();
            throw new MojoExecutionException("Unable to process S3 file(s)");
        }

        getLog().info("Successfully processed all S3 objects");
    }

    /**
     * Register custom s3:// protocol
     *
     * Based on <http://stackoverflow.com/a/26409796>
     */
    protected static void registerS3ProtocolHandler() {
        URL.setURLStreamHandlerFactory(protocol -> "s3".equals(protocol) ? new URLStreamHandler() {
            protected URLConnection openConnection(URL url) throws IOException {
                return new URLConnection(url) {
                    public void connect() throws IOException {
                    }
                };
            }
        } : null);
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

    protected void process(List<info.lindblad.maven.plugins.aws.s3.S3Object> s3Objects) throws IOException, MojoExecutionException {
        for (info.lindblad.maven.plugins.aws.s3.S3Object s3Object : s3Objects) {
            process(s3Object);
        }
    }

    /**
     * Process S3 objects
     *
     * @param s3Object S3 object(s) to process
     * @throws MojoExecutionException
     * @throws IOException
     */
    private void process(info.lindblad.maven.plugins.aws.s3.S3Object s3Object) throws MojoExecutionException, IOException {
        File destinationFile = new File(s3Object.destination);

        if (s3Object.isDestinationDirectory() || s3Object.isSourceDirectory()) {
            getLog().error("There is currently no support for directories. Please only refer to files.");
            throw new NotImplementedException();
        }
        process(s3Object.getBucketName(), s3Object.getObjectKey(), destinationFile, s3Object.shouldOverwrite());
    }


    /**
     * Download a single file from S3
     *
     *
     * @param bucketName      The S3 bucket name
     * @param key             The S3 key
     * @param destination     The local destination
     * @param shouldOverwrite Whether to overwrite existing files
     * @throws IOException
     */
    private void process(String bucketName, String key, File destination, boolean shouldOverwrite) throws IOException {
        getLog().info(String.format("Downloading s3://%s/%s to %s", bucketName, key, destination.getName()));


        if (destination.exists() && !destination.isDirectory() && !shouldOverwrite) {
            getLog().info(String.format("Skipping existing file: %s", destination.getName()));
        } else {
            GetObjectRequest request = new GetObjectRequest(bucketName, key);

            com.amazonaws.services.s3.model.S3Object object = s3Client.getObject(request);
            S3ObjectInputStream objectContent = object.getObjectContent();

            if (destination.exists()) {
                getLog().info(String.format("Overwriting existing s3Object: %s", destination.getName()));
            }
            Files.copy(objectContent, destination.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }

}
