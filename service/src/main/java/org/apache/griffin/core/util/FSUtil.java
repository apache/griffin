/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package org.apache.griffin.core.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Component
public class FSUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(FSUtil.class);

    private static String fsDefaultName;

    private static FileSystem fileSystem;

    private static FileSystem getFileSystem() {
        if (fileSystem == null) {
            initFileSystem();
        }
        return fileSystem;
    }

    public FSUtil(@Value("${fs.defaultFS}") String defaultName) {
        fsDefaultName = defaultName;
    }


    private static void initFileSystem() {
        Configuration conf = new Configuration();
        if (!StringUtils.isEmpty(fsDefaultName)) {
            conf.set("fs.defaultFS", fsDefaultName);
            LOGGER.info("Setting fs.defaultFS:{}", fsDefaultName);
        }
        if (StringUtils.isEmpty(conf.get("fs.hdfs.impl"))) {
            LOGGER.info("Setting fs.hdfs.impl:{}", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        }
        if (StringUtils.isEmpty(conf.get("fs.file.impl"))) {
            LOGGER.info("Setting fs.hdfs.impl:{}", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        }
        try {
            fileSystem = FileSystem.get(conf);
        } catch (Exception e) {
            LOGGER.error("Can not get hdfs file system.", e);
        }

    }


    /**
     * list all sub dir of a dir
     */
    public static List<String> listSubDir(String dir) throws IOException {
        if (getFileSystem() == null) {
            throw new NullPointerException("FileSystem is null.Please check your hdfs config default name.");
        }
        List<String> fileList = new ArrayList<>();
        Path path = new Path(dir);
        if (fileSystem.isFile(path)) {
            return fileList;
        }
        FileStatus[] statuses = fileSystem.listStatus(path);
        for (FileStatus fileStatus : statuses) {
            if (fileStatus.isDirectory()) {
                fileList.add(fileStatus.getPath().toString());
            }
        }
        return fileList;

    }

    /**
     * get all file status of a dir.
     */
    public static List<FileStatus> listFileStatus(String dir) throws IOException {
        if (getFileSystem() == null) {
            throw new NullPointerException("FileSystem is null.Please check your hdfs config default name.");
        }
        List<FileStatus> fileStatusList = new ArrayList<>();
        Path path = new Path(dir);
        if (fileSystem.isFile(path)) {
            return fileStatusList;
        }
        FileStatus[] statuses = fileSystem.listStatus(path);
        for (FileStatus fileStatus : statuses) {
            if (!fileStatus.isDirectory()) {
                fileStatusList.add(fileStatus);
            }
        }
        return fileStatusList;
    }

    /**
     * touch file
     */
    public static void touch(String filePath) throws IOException {
        if (getFileSystem() == null) {
            throw new NullPointerException("FileSystem is null.Please check your hdfs config default name.");
        }
        Path path = new Path(filePath);
        FileStatus st;
        if (fileSystem.exists(path)) {
            st = fileSystem.getFileStatus(path);
            if (st.isDirectory()) {
                throw new IOException(filePath + " is a directory");
            } else if (st.getLen() != 0) {
                throw new IOException(filePath + " must be a zero-length file");
            }
        }
        FSDataOutputStream out = null;
        try {
            out = fileSystem.create(path);
        } finally {
            if (out != null) {
                out.close();
            }
        }

    }


    public static boolean isFileExist(String path) throws IOException {
        if (getFileSystem() == null) {
            throw new NullPointerException("FileSystem is null.Please check your hdfs config default name.");
        }
        Path hdfsPath = new Path(path);
        return fileSystem.isFile(hdfsPath) || fileSystem.isDirectory(hdfsPath);
    }

}
