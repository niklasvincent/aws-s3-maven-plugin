package info.lindblad.maven.plugins.aws.s3;


import java.net.MalformedURLException;
import java.net.URL;

public class S3Object {

    public String source;
    public String destination;
    public boolean overwrite = false;

    private URL url;

    private void ensureSourceIsParsedAsUrl() throws MalformedURLException {
        if (null == url) {
            url = new URL(source);
        }
    }

    /**
     * Check whether a path is a directory or not depending on trailing slash
     *
     * @param path The path to inspect
     * @return Whether the path is a directory or not
     */
    public static boolean isDirectory(String path) {
        return path.charAt(path.length() - 1) == '/';
    }

    public String getBucketName() throws MalformedURLException {
        ensureSourceIsParsedAsUrl();
        return url.getHost();
    }

    public String getObjectKey() throws MalformedURLException {
        ensureSourceIsParsedAsUrl();
        return url.getPath().substring(1);
    }

    public String getDestination() {
        return destination;
    }

    public boolean shouldOverwrite() {
        return overwrite;
    }

    public boolean isDestinationDirectory() {
        return isDirectory(destination);
    }

    public boolean isSourceDirectory() {
        return isDirectory(source);
    }

}
