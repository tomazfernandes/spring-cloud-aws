package io.awspring.cloud.s3;

import java.io.IOException;
import java.io.InputStream;

public interface S3Operations {

	String createBucket(String bucketName);

	void deleteBucket(String bucketName);

	void deleteObject(String bucketName, String key);

	void deleteObject(String s3Url);

	void upload(String bucketName, String key, InputStream inputStream) throws IOException;

}
