// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.aws;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
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

    public boolean downloadObj(String bucketName, String key, String localDirectory) {
        try {
            final S3Object s3Object = awsConfig.getAmazonS3().getObject(new GetObjectRequest(bucketName, key));
            final Path target = Paths.get(localDirectory, key);
            Files.createDirectories(target.getParent());
            Files.copy(s3Object.getObjectContent(), target, StandardCopyOption.REPLACE_EXISTING);
        } catch (AmazonS3Exception | IOException e) {
            LOG.error("download obj fail, bucket: {}, key: {}", bucketName, key, e);
            return false;
        }
        return true;
    }
}
