// Copyright 2021 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.cloud.huawei;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.obs.services.ObsClient;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObsObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class HuaweiObsExecutor {
    private static final Logger LOG = LogManager.getLogger();

    private final ObsClient obsClient;

    public HuaweiObsExecutor(HuaweiCloudClient client) {
        this.obsClient = client.getObsClient();
    }

    public void uploadObject(String bucketName, String key, File file) {
        try {
            obsClient.putObject(bucketName, key, file);
        } catch (AmazonS3Exception e) {
            LOG.error("upload obj fail, bucket: {}, key: {}, file: {}", bucketName, key, file.getAbsolutePath(), e);
            throw new RuntimeException("upload obj fail", e);
        }
    }

    public Path downloadDliQueryResultObj(String bucketName, String objectKey, String localDir, String year, String month, String day) {
        String date = year + month + day;
        Path dliQueryResultPath = Paths.get(localDir, date, objectKey);
        try {
            Files.createDirectories(dliQueryResultPath);
        } catch (IOException e) {
            LOG.error("download dli query result, mkdir fail, obs bucket: {}, object key: {}, local path: {}", bucketName, objectKey, dliQueryResultPath, e);
            throw new RuntimeException("download obj fail", e);
        }
        LOG.info("download dli query result, start..., obs bucket: {}, object key: {}, local path: {}", bucketName, objectKey, dliQueryResultPath);
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucketName, objectKey, null, null, 1000);
        ObjectListing objectListing;
        do {
            objectListing = obsClient.listObjects(listObjectsRequest);
            for (ObsObject obsObject : objectListing.getObjects()) {
                Long contentLength = obsObject.getMetadata().getContentLength();
                String currentObjectKey = obsObject.getObjectKey();
                if (contentLength == 0 || !currentObjectKey.endsWith("csv")) {
                    continue;
                }
                Path target = Paths.get(localDir, date, currentObjectKey);
                try (InputStream objectContent = obsClient.getObject(bucketName, currentObjectKey).getObjectContent()) {
                    Files.copy(objectContent, target, StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    LOG.error("download dli query result fail, obs bucket: {}, object key: {}, local path: {}", bucketName, currentObjectKey, target, e);
                    throw new RuntimeException("download obj fail", e);
                }
            }
            listObjectsRequest.setMarker(objectListing.getNextMarker());
        } while (objectListing.isTruncated());
        LOG.info("download dli query result complete, obs path: {}/{}, local path: {} ", bucketName, objectKey, dliQueryResultPath);
        return dliQueryResultPath;
    }
}
