package kmeans;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class FileUtility {

    private static final String S3_FILE_PREFIX = "s3://";
    public static List<String> getListOfFilesInDir(String bucket, String dir, boolean isStorageS3) throws IOException {
        System.out.println("It is calling the function");
        System.out.println(bucket);
        System.out.println(dir);

        List<String> files = new ArrayList();
        if (isStorageS3) {
            System.setProperty("aws.accessKeyId", "AKIAIW37ONIBMB3EID2Q");
            System.setProperty("aws.secretKey", "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");
            final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
            System.err.println(bucket);
            System.err.println(dir);
            ObjectListing listing = s3.listObjects(bucket, dir);
            List<S3ObjectSummary> summaries = listing.getObjectSummaries();
            for(S3ObjectSummary s : summaries) {
                if(!(s.getKey().startsWith(".") || s.getKey().startsWith("_")))
                    files.add(S3_FILE_PREFIX + bucket + "/" + s.getKey());
            }
        } else {
            Files.walk(Paths.get(dir))
                    .filter(Files::isRegularFile)
                    .filter(p -> !(p.getFileName().toString().startsWith(".") || p.getFileName().toString().startsWith("_")) )
                    .forEach(p -> {
                        files.add("file:///" + p.toString().replaceAll("\\\\","/"));});
        }
        return files;
    }
}