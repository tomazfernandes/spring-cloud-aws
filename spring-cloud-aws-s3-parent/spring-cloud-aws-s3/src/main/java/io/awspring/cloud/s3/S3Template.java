package io.awspring.cloud.s3;

import java.io.IOException;
import java.io.InputStream;

import software.amazon.awssdk.services.s3.S3Client;

import org.springframework.util.StreamUtils;

public class S3Template implements S3Operations {

	private final S3Client s3Client;

	private final S3OutputStreamProvider s3OutputStreamProvider;

	public S3Template(S3Client s3Client, S3OutputStreamProvider s3OutputStreamProvider) {
		this.s3Client = s3Client;
		this.s3OutputStreamProvider = s3OutputStreamProvider;
	}

	@Override
	public String createBucket(String bucketName) {
		return s3Client.createBucket(request -> request.bucket(bucketName)).location();
	}

	@Override
	public void deleteBucket(String bucketName) {
		s3Client.deleteBucket(request -> request.bucket(bucketName));
	}

	@Override
	public void deleteObject(String bucketName, String key) {
		s3Client.deleteObject(request -> request.bucket(bucketName).key(key));
	}

	@Override
	public void deleteObject(String s3Url) {
		Location location = Location.of(s3Url);
		this.deleteObject(location.getBucket(), location.getObject());
	}

	@Override
	public void upload(String bucketName, String key, InputStream inputStream) throws IOException {
		S3Resource s3Resource = new S3Resource(bucketName, key, s3Client, s3OutputStreamProvider);
		StreamUtils.copy(inputStream, s3Resource.getOutputStream());
	}

}
