/**
 * Project		: AWSSupport
 * FileName		: S3Support.java
 * Package		: com.khan.libs.aws
 * 
 * @brief		: 
 * @author		: KHAN
 * @date		: 2013. 6. 4. 오후 3:13:48
 */
package com.khan.libs.aws.s3;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;

/**
 * <PRE>
 * AWS Simple Storage Service Support class.
 * 
 * @class	: com.khan.libs.aws.S3Support
 * @file	: S3Support.java
 * @author	: KHAN
 * @date	: 2013. 6. 4. 오후 3:13:48
 * </PRE>
 */
public class S3Support {
	private static final Log log = LogFactory.getLog(S3Support.class);
	
	/** The bucket name. */
	private String bucketName;
	
	/** The aws credentials provider. */
	private AWSCredentialsProvider awsCredentialsProvider;
	
	/** The is s3 enabled. */
	private boolean s3Enabled = false;
	
	/** The region. */
	private Region region = Region.getRegion(Regions.AP_NORTHEAST_1);
	
	/**
	 * Instantiates a new s3 support.
	 */
	public S3Support(){}
	
	/**
	 * Instantiates a new s3 support.
	 *
	 * @param bucketName the bucket name
	 */
	public S3Support(String bucketName) {
		this(bucketName, new ClasspathPropertiesFileCredentialsProvider());
	}
	
	/**
	 * Instantiates a new s3 support.
	 *
	 * @param bucketName the bucket name
	 * @param provider the provider
	 */
	public S3Support(String bucketName, AWSCredentialsProvider provider) {
		setBucketName(bucketName);
		setAWSCredentialsProvider(provider);
	}
	
	/**
	 * Sets the bucket name.
	 *
	 * @param bucketName the new bucket name
	 */
	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}
	
	/**
	 * Sets the aWS credentials provider.
	 *
	 * @param provider the new aWS credentials provider
	 */
	public void setAWSCredentialsProvider(AWSCredentialsProvider provider) {
		this.awsCredentialsProvider = provider;
		init();
	}
	
	/**
	 * Sets the region.
	 *
	 * @param regions the new region
	 */
	public void setRegion(Regions regions) {
		this.region = Region.getRegion(regions);
	}
	
	/**
	 * Checks if is s3 enabled.
	 *
	 * @return true, if is s3 enabled
	 */
	public boolean isS3Enabled() {
		return s3Enabled;
	}
	
	/**
	 * Initialize AWS Credentials.
	 */
	public void init() {
		try {
			AWSCredentials credentials = awsCredentialsProvider.getCredentials();
			if (credentials != null 
					&& "".equals(credentials.getAWSAccessKeyId().trim()) 
					&& "".equals(credentials.getAWSSecretKey().trim())) {
				s3Enabled = true;
			}
		} catch (Exception e) {
			log.warn("S3 credential not defined....", e);
			s3Enabled = false;
		}
	}
	
	/**
	 * S3 클라이언트를 생성
	 *
	 * @return the s3 client
	 */
	public AmazonS3Client getS3Client() {
		if (awsCredentialsProvider == null)
			throw new IllegalArgumentException("AWSCredentialsProvider must not null!");
		AmazonS3Client s3 = new AmazonS3Client(awsCredentialsProvider);
		s3.setRegion(region);
		return s3;
	}
	
	/**
	 * S3 버켓에 파일 저장
	 *
	 * @param filename S3 버켓에 저장될 파일의 경로 및 이름
	 * @param orgFilename S3 버켓에 저장할 본래 파일의 경로 및 이름
	 * @return true, if successful
	 */
	public boolean saveFileToS3(String filename, String orgFilename) {
		File file = new File(orgFilename);
		return saveFileToS3(filename, file);
	}

	
	/**
	 * S3 버켓에 파일 저장
	 *
	 * @param filename S3 버켓에 저장될 파일의 경로 및 이름
	 * @param file S3 버켓에 저장할 파일
	 * @return true, if successful
	 */
	public boolean saveFileToS3(String filename, File file) {
		boolean result = false;
		
		AmazonS3Client s3 = getS3Client();
		
		s3.putObject(bucketName, filename, file);
		
		return result;
	}
	
	/**
	 * S3 버켓에 파일 저장
	 *
	 * @param filename S3 버켓에 저장될 파일의 경로 및 이름
	 * @param bytes S3 버켓에 저장할 파일의 byte 데이터
	 * @return true, if successful
	 */
	public boolean saveFileToS3(String filename, byte[] bytes) {
		InputStream is = new ByteArrayInputStream(bytes);
		return saveFileToS3(filename, is);
	}
	
	/**
	 * S3 버켓에 파일 저장
	 *
	 * @param filename S3 버켓에 저장될 파일의 경로 및 이름
	 * @param is 저장할 파일의 InputStream
	 * @return true, if successful
	 */
	public boolean saveFileToS3(String filename, InputStream is) {
		boolean result = false;
		
		AmazonS3Client s3 = getS3Client();
		
		s3.putObject(bucketName, filename, is, new ObjectMetadata());
		return result;
	}
	
	/**
	 * 버켓을 새로 생성
	 *
	 * @param bucketName 생성할 버켓의 이름
	 * @return true, if successful
	 */
	public boolean createBucket(String bucketName) {
		AmazonS3Client s3 = getS3Client();

		Bucket bucket = s3.createBucket(bucketName);
		
		this.bucketName = bucket.getName();
		
		return true;
	}
	
	/**
	 * S3 버켓 이름을 가져옴. 
	 *
	 * @return the bucket names
	 */
	public Set<String> getBucketNames() {
		AmazonS3Client s3 = getS3Client();
		
		Set<String> bucketNames = new HashSet<String>();
		for (Bucket bucket : s3.listBuckets()) {
			bucketNames.add(bucket.getName());
		}
		return bucketNames;
	}
}