# HDFSFileManager 实现说明

以下为根据 `res.md` 中给出的代码框架完成的 `HDFSFileManager` 全量实现。所有 `TODO` 注释均被保留，并在其下方补充了对应的实现代码，便于对照学习与调试。

```java
package com.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Objects;

/**
 * HDFS 文件管理器
 * 提供基本的 HDFS 文件操作功能
 */
public class HDFSFileManager {
    private static final Logger logger = LoggerFactory.getLogger(HDFSFileManager.class);

    private FileSystem fileSystem;
    private Configuration configuration;

    /**
     * 构造函数，初始化 HDFS 连接
     * @param hdfsUri HDFS 的 URI，例如 "hdfs://localhost:9000"
     */
    public HDFSFileManager(String hdfsUri) throws IOException {
        // TODO: 实现构造函数
        // 1. 创建 Configuration 对象
        this.configuration = new Configuration();
        // 2. 设置 HDFS URI
        this.configuration.set("fs.defaultFS", Objects.requireNonNull(hdfsUri, "hdfsUri 不能为空"));
        // 3. 获取 FileSystem 实例
        this.fileSystem = FileSystem.get(URI.create(hdfsUri), configuration);
        logger.info("已连接到 {}", hdfsUri);
    }

    /**
     * 上传本地文件到 HDFS
     * @param localPath 本地文件路径
     * @param hdfsPath HDFS 目标路径
     * @param overwrite 是否覆盖已存在的文件
     * @return 上传是否成功
     */
    public boolean uploadFile(String localPath, String hdfsPath, boolean overwrite) {
        // TODO: 实现文件上传功能
        // 1. 检查本地文件是否存在
        File source = new File(localPath);
        if (!source.exists() || !source.isFile()) {
            logger.warn("本地文件不存在：{}", localPath);
            return false;
        }
        // 2. 创建 HDFS 目标目录（如果不存在）
        Path target = new Path(hdfsPath);
        try {
            Path parent = target.getParent();
            if (parent != null && !fileSystem.exists(parent)) {
                fileSystem.mkdirs(parent);
            }
        // 3. 执行文件上传
            fileSystem.copyFromLocalFile(false, overwrite, new Path(source.getAbsolutePath()), target);
            logger.info("已上传 {} -> {}", localPath, hdfsPath);
            return true;
        // 4. 处理异常情况
        } catch (IOException e) {
            logger.error("上传 {} 到 {} 失败", localPath, hdfsPath, e);
            return false;
        }
    }

    /**
     * 从 HDFS 下载文件到本地
     * @param hdfsPath HDFS 文件路径
     * @param localPath 本地目标路径
     * @param overwrite 是否覆盖已存在的文件
     * @return 下载是否成功
     */
    public boolean downloadFile(String hdfsPath, String localPath, boolean overwrite) {
        // TODO: 实现文件下载功能
        Path source = new Path(hdfsPath);
        File target = new File(localPath);
        try {
            if (!fileSystem.exists(source)) {
                logger.warn("HDFS 文件不存在：{}", hdfsPath);
                return false;
            }
            if (target.exists() && !overwrite) {
                logger.warn("本地文件已存在且未设置覆盖：{}", localPath);
                return false;
            }
            File parent = target.getParentFile();
            if (parent != null && !parent.exists() && !parent.mkdirs()) {
                logger.warn("无法创建本地目录：{}", parent.getAbsolutePath());
            }
            try (FSDataInputStream inputStream = fileSystem.open(source);
                 OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(target))) {
                IOUtils.copyBytes(inputStream, outputStream, configuration, false);
            }
            logger.info("已下载 {} -> {}", hdfsPath, localPath);
            return true;
        } catch (IOException e) {
            logger.error("下载 {} 到 {} 失败", hdfsPath, localPath, e);
            return false;
        }
    }

    /**
     * 删除 HDFS 中的文件或目录
     * @param hdfsPath HDFS 路径
     * @param recursive 是否递归删除（用于目录）
     * @return 删除是否成功
     */
    public boolean deleteFile(String hdfsPath, boolean recursive) {
        // TODO: 实现文件删除功能
        Path target = new Path(hdfsPath);
        try {
            if (!fileSystem.exists(target)) {
                logger.warn("待删除路径不存在：{}", hdfsPath);
                return false;
            }
            boolean deleted = fileSystem.delete(target, recursive);
            logger.info("删除 {}：{}", hdfsPath, deleted ? "成功" : "失败");
            return deleted;
        } catch (IOException e) {
            logger.error("删除 {} 失败", hdfsPath, e);
            return false;
        }
    }

    /**
     * 递归列出目录中的所有文件和子目录
     * @param hdfsPath HDFS 目录路径
     * @param depth 当前递归深度（用于格式化输出）
     */
    public void listDirectory(String hdfsPath, int depth) {
        // TODO: 实现目录遍历功能
        Path path = new Path(hdfsPath);
        // 1. 检查路径是否存在
        try {
            if (!fileSystem.exists(path)) {
                logger.warn("路径不存在：{}", hdfsPath);
                return;
            }
            FileStatus status = fileSystem.getFileStatus(path);
        // 2. 获取目录内容
            printStatus(status, depth);
        // 3. 递归处理子目录
            if (status.isDirectory()) {
                for (FileStatus child : fileSystem.listStatus(path)) {
                    listDirectory(child.getPath().toString(), depth + 1);
                }
            }
        // 4. 格式化输出结果
        } catch (IOException e) {
            logger.error("遍历目录 {} 失败", hdfsPath, e);
        }
    }

    private void printStatus(FileStatus status, int depth) {
        StringBuilder prefix = new StringBuilder();
        for (int i = 0; i < depth; i++) {
            prefix.append("  ");
        }
        String type = status.isDirectory() ? "[DIR]" : "[FILE]";
        logger.info("{}{} {}", prefix, type, status.getPath());
    }

    /**
     * 统计目录信息
     * @param hdfsPath HDFS 目录路径
     * @return DirectoryStats 对象，包含统计信息
     */
    public DirectoryStats getDirectoryStats(String hdfsPath) {
        // TODO: 实现目录统计功能
        DirectoryStats stats = new DirectoryStats();
        Path path = new Path(hdfsPath);
        try {
            if (!fileSystem.exists(path)) {
                logger.warn("路径不存在：{}", hdfsPath);
                return stats;
            }
            accumulateStats(path, stats);
        } catch (IOException e) {
            logger.error("统计目录 {} 失败", hdfsPath, e);
        }
        return stats;
    }

    private void accumulateStats(Path path, DirectoryStats stats) throws IOException {
        FileStatus status = fileSystem.getFileStatus(path);
        if (status.isDirectory()) {
            stats.incrementDirectory();
            for (FileStatus child : fileSystem.listStatus(path)) {
                accumulateStats(child.getPath(), stats);
            }
        } else {
            stats.incrementFile(status.getLen());
        }
    }

    /**
     * 关闭 HDFS 连接
     */
    public void close() {
        // TODO: 实现资源清理
        IOUtils.closeStream(fileSystem);
        fileSystem = null;
        configuration = null;
        logger.info("HDFS 连接已关闭");
    }

    /**
     * 目录统计信息类
     */
    public static class DirectoryStats {
        private long fileCount;
        private long directoryCount;
        private long totalSize;

        // TODO: 实现构造函数、getter 和 toString 方法
        public DirectoryStats() {
        }

        public DirectoryStats(long fileCount, long directoryCount, long totalSize) {
            this.fileCount = fileCount;
            this.directoryCount = directoryCount;
            this.totalSize = totalSize;
        }

        public void incrementFile(long size) {
            this.fileCount++;
            this.totalSize += size;
        }

        public void incrementDirectory() {
            this.directoryCount++;
        }

        public long getFileCount() {
            return fileCount;
        }

        public long getDirectoryCount() {
            return directoryCount;
        }

        public long getTotalSize() {
            return totalSize;
        }

        @Override
        public String toString() {
            return String.format("files=%d, directories=%d, size=%d bytes", fileCount, directoryCount, totalSize);
        }
    }

    /**
     * 主方法，用于测试 HDFS 文件管理器
     */
    public static void main(String[] args) {
        HDFSFileManager manager = null;
        try {
            // TODO: 实现完整的测试流程
            // 1. 创建 HDFSFileManager 实例
            String hdfsUri = args.length > 0 ? args[0] : "hdfs://localhost:9000";
            String workingDir = args.length > 1 ? args[1] : "/user/student/project/demo";
            String localSample = args.length > 2 ? args[2] : "sample-upload.txt";
            createLocalSample(localSample);
            manager = new HDFSFileManager(hdfsUri);
            String hdfsSample = workingDir + "/sample.txt";
            // 2. 测试文件上传功能
            manager.uploadFile(localSample, hdfsSample, true);
            // 3. 测试目录遍历功能
            manager.listDirectory(workingDir, 0);
            // 4. 测试文件下载功能
            manager.downloadFile(hdfsSample, "downloaded-sample.txt", true);
            // 5. 测试目录统计功能
            DirectoryStats stats = manager.getDirectoryStats(workingDir);
            logger.info("目录统计结果：{}", stats);
            // 6. 测试文件删除功能
            manager.deleteFile(hdfsSample, false);
        } catch (Exception e) {
            logger.error("程序执行出错", e);
        } finally {
            if (manager != null) {
                manager.close();
            }
        }
    }

    private static void createLocalSample(String localPath) throws IOException {
        File file = new File(localPath);
        if (!file.exists()) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                writer.write("Sample line 1\n");
                writer.write("Sample line 2\n");
                writer.write("Sample line 3\n");
            }
        }
    }
}
```
