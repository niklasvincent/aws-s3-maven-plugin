package info.lindblad.maven.plugins.aws.s3;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;


public class S3ObjectTest {

    @BeforeClass
    public static void setUp() {
        AWSS3Mojo.registerS3ProtocolHandler();
    }

    /**
     * Maven MOJOs need to have public constructors with zero arguments
     */
    @Test
    public void testHasPublicConstructor() {
        new AWSS3Mojo();
    }

    @Test
    public void isDirectory() throws Exception {
        assertTrue(S3Object.isDirectory("some/directory/"));
        assertFalse(S3Object.isDirectory("some/file"));
    }

    @Test
    public void getBucketName() throws Exception {
        S3Object s3Object = new S3Object();
        s3Object.source = "s3://my-bucket/some/directory/file";
        s3Object.destination = "/local/file";
        s3Object.overwrite = true;
        assertEquals("my-bucket", s3Object.getBucketName());
    }

    @Test
    public void getObjectKey() throws Exception {
        S3Object s3Object = new S3Object();
        s3Object.source = "s3://my-bucket/some/directory/file";
        s3Object.destination = "/local/file";
        s3Object.overwrite = true;
        assertEquals("some/directory/file", s3Object.getObjectKey());
    }

    @Test
    public void getDestination() throws Exception {
        S3Object s3Object = new S3Object();
        s3Object.source = "s3://my-bucket/some/directory/file";
        s3Object.destination = "/local/file";
        s3Object.overwrite = true;
        assertEquals("/local/file", s3Object.getDestination());
    }

    @Test
    public void shouldOverwrite() throws Exception {
        S3Object s3ObjectWithOverwrite = new S3Object();
        s3ObjectWithOverwrite.source = "s3://my-bucket/some/directory/file";
        s3ObjectWithOverwrite.destination = "/local/file";
        s3ObjectWithOverwrite.overwrite = true;
        assertTrue(s3ObjectWithOverwrite.shouldOverwrite());

        S3Object s3ObjectWithoutOverwrite = new S3Object();
        s3ObjectWithoutOverwrite.source = "s3://my-bucket/some/directory/file";
        s3ObjectWithoutOverwrite.destination = "/local/file";
        s3ObjectWithoutOverwrite.overwrite = false;
        assertFalse(s3ObjectWithoutOverwrite.shouldOverwrite());
    }

}