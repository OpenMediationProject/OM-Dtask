// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.aws;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

@Component
public class S3Executor {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private AwsConfig awsConfig;

    public void downloadObj(String bucketName, String key, String localDirectory) {
        try {
            final S3Object s3Object = awsConfig.getAmazonS3().getObject(new GetObjectRequest(bucketName, key));
            final Path target = Paths.get(localDirectory, key);
            Files.createDirectories(target.getParent());
            Files.copy(s3Object.getObjectContent(), target, StandardCopyOption.REPLACE_EXISTING);
        } catch (AmazonS3Exception | IOException e) {
            LOG.error("download obj fail, bucket: {}, key: {}", bucketName, key, e);
            throw new RuntimeException("download obj fail", e);
        }
    }

    public void uploadObject(String bucketName, String key, File file) {
        try {
            awsConfig.getAmazonS3().putObject(bucketName, key, file);
        } catch (AmazonS3Exception e) {
            LOG.error("upload obj fail, bucket: {}, key: {}, file: {}", bucketName, key, file.getAbsolutePath(), e);
            throw new RuntimeException("upload obj fail", e);
        }
    }

    public void uploadObject(String bucketName, String key, File file, ObjectMetadata objectMetadata) {
        try {
            awsConfig.getAmazonS3().putObject(new PutObjectRequest(bucketName, key, file).withMetadata(objectMetadata));
        } catch (AmazonS3Exception e) {
            LOG.error("upload obj fail, bucket: {}, key: {}, file: {}", bucketName, key, file.getAbsolutePath(), e);
            throw new RuntimeException("upload obj fail", e);
        }
    }

    public Path downloadAthenaQueryResultObj(String localDirectory, String bucketName, String year, String month, String day, String queryExecutionId) {
        String queryResultKeyOnS3 = String.join("/", AthenaConstants.DATA_PATH_ATHENA, year, month, day, queryExecutionId + ".csv");
        Path target = Paths.get(localDirectory, year + month + day, AthenaConstants.DATA_PATH_ATHENA, queryExecutionId + ".csv");
        LOG.info("download athena query result start..., s3 path: {}/{}, local path: {} ", awsConfig.getS3Bucket(), queryResultKeyOnS3, target);
        try {
            final S3Object s3Object = awsConfig.getAmazonS3().getObject(new GetObjectRequest(bucketName, queryResultKeyOnS3));
            Files.createDirectories(target.getParent());
            Files.copy(s3Object.getObjectContent(), target, StandardCopyOption.REPLACE_EXISTING);
        } catch (AmazonS3Exception | IOException e) {
            LOG.error("download obj fail, bucket: {}, key: {}", bucketName, queryResultKeyOnS3, e);
            throw new RuntimeException("download obj fail", e);
        }
        LOG.info("download athena query result complete, s3 path: {}/{}, local path: {} ", awsConfig.getS3Bucket(), queryResultKeyOnS3, target);
        return target;
    }
}
