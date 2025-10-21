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
import java.util.Arrays;
import java.util.Comparator;
import java.util.Locale;
import java.util.Objects;
import java.util.Scanner;

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
        } catch (IOException e) {
            // 4. 处理异常情况
            logger.error("上传 {} 到 {} 失败", localPath, hdfsPath, e);
            return false;
        }
    }

    /**
     * 从 HDFS 下载文件到本地
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
            if (target.isDirectory() || localPath.endsWith(File.separator) || localPath.endsWith("/")) {
                target = new File(target, source.getName());
            }
            if (target.exists() && !overwrite) {
                logger.warn("本地文件已存在且未设置覆盖：{}", target.getAbsolutePath());
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
            logger.info("已下载 {} -> {}", hdfsPath, target.getAbsolutePath());
            return true;
        } catch (IOException e) {
            logger.error("下载 {} 到 {} 失败", hdfsPath, localPath, e);
            return false;
        }
    }

    /**
     * 删除 HDFS 中的文件或目录
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
     */
    public void listDirectory(String hdfsPath, int depth) {
        // TODO: 实现目录遍历功能
        // 1. 检查路径是否存在
        Path path = new Path(hdfsPath);
        try {
            if (!fileSystem.exists(path)) {
                logger.warn("路径不存在：{}", hdfsPath);
                return;
            }
            FileStatus status = fileSystem.getFileStatus(path);
            String rootName = status.getPath().toUri().getPath();
            if (rootName == null || rootName.isEmpty()) {
                rootName = "/";
            }
            // 4. 格式化输出结果
            System.out.println(rootName + (status.isDirectory() ? "/" : ""));
            if (status.isDirectory()) {
                printTreeChildren(path, "");
            }
        } catch (IOException e) {
            logger.error("遍历目录 {} 失败", hdfsPath, e);
        }
    }

    private void printTreeChildren(Path path, String prefix) throws IOException {
        FileStatus[] children = fileSystem.listStatus(path);
        Arrays.sort(children, Comparator.comparing(status -> status.getPath().getName()));
        for (int i = 0; i < children.length; i++) {
            FileStatus child = children[i];
            boolean isLast = i == children.length - 1;
            String connector = isLast ? "└── " : "├── ";
            String name = child.getPath().getName();
            if (name == null || name.isEmpty()) {
                name = child.getPath().toUri().getPath();
            }
            System.out.println(prefix + connector + name + (child.isDirectory() ? "/" : ""));
            if (child.isDirectory()) {
                String childPrefix = prefix + (isLast ? "    " : "│   ");
                printTreeChildren(child.getPath(), childPrefix);
            }
        }
    }

    /**
     * 统计目录信息
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
        Scanner scanner = new Scanner(System.in);
        try {
            // TODO: 实现完整的测试流程
            // 1. 创建 HDFSFileManager 实例
            String defaultUri = "hdfs://localhost:9000";
            String hdfsUri = args.length > 0
                    ? args[0]
                    : promptWithDefault(scanner, "请输入 HDFS URI（直接回车使用默认值 " + defaultUri + "）：", defaultUri);
            manager = new HDFSFileManager(hdfsUri);

            boolean running = true;
            while (running) {
                printMenu();
                String choice = scanner.nextLine().trim().toLowerCase(Locale.ROOT);
                switch (choice) {
                    case "1":
                    case "upload":
                        handleUpload(scanner, manager);
                        break;
                    case "2":
                    case "list":
                        handleList(scanner, manager);
                        break;
                    case "3":
                    case "download":
                        handleDownload(scanner, manager);
                        break;
                    case "4":
                    case "stats":
                        handleStats(scanner, manager);
                        break;
                    case "5":
                    case "delete":
                        handleDelete(scanner, manager);
                        break;
                    case "6":
                    case "exit":
                        running = false;
                        System.out.println("感谢使用，程序即将退出。");
                        break;
                    default:
                        System.out.println("未知操作，请重新选择。");
                        break;
                }
            }
        } catch (Exception e) {
            logger.error("程序执行出错", e);
        } finally {
            if (manager != null) {
                manager.close();
            }
        }
    }

    private static void printMenu() {
        System.out.println();
        System.out.println("====== HDFS 文件管理菜单 ======");
        System.out.println("1. upload   - 上传本地文件到 HDFS");
        System.out.println("2. list     - 遍历并列出 HDFS 目录");
        System.out.println("3. download - 下载 HDFS 文件到本地");
        System.out.println("4. stats    - 统计 HDFS 目录信息");
        System.out.println("5. delete   - 删除 HDFS 文件或目录");
        System.out.println("6. exit     - 退出程序");
        System.out.print("请选择操作（输入数字或命令）：");
    }

    private static void handleUpload(Scanner scanner, HDFSFileManager manager) {
        String localPath = promptRequired(scanner, "请输入本地文件路径");
        if (localPath == null) {
            System.out.println("已返回主菜单。");
            return;
        }
        String targetPath = promptRequired(scanner, "请输入 HDFS 目标文件路径");
        if (targetPath == null) {
            System.out.println("已返回主菜单。");
            return;
        }
        Boolean overwrite = promptConfirm(scanner, "若目标已存在是否覆盖？(y/N)", false);
        if (overwrite == null) {
            System.out.println("已返回主菜单。");
            return;
        }
        boolean success = manager.uploadFile(localPath, targetPath, overwrite);
        System.out.println(success ? "上传成功。" : "上传失败，请查看日志获取详细信息。");
    }

    private static void handleList(Scanner scanner, HDFSFileManager manager) {
        String hdfsDir = "/";
        System.out.println("正在遍历 HDFS 目录：" + hdfsDir);
        manager.listDirectory(hdfsDir, 0);
        System.out.println("目录遍历完成，上方树状结构展示了当前 HDFS 目录。");
    }

    private static void handleDownload(Scanner scanner, HDFSFileManager manager) {
        String hdfsPath = promptRequired(scanner, "请输入要下载的 HDFS 文件路径");
        if (hdfsPath == null) {
            System.out.println("已返回主菜单。");
            return;
        }
        String localPath = promptRequired(scanner, "请输入本地保存路径");
        if (localPath == null) {
            System.out.println("已返回主菜单。");
            return;
        }
        Boolean overwrite = promptConfirm(scanner, "若本地文件存在是否覆盖？(y/N)", false);
        if (overwrite == null) {
            System.out.println("已返回主菜单。");
            return;
        }
        boolean success = manager.downloadFile(hdfsPath, localPath, overwrite);
        System.out.println(success ? "下载成功。" : "下载失败，请查看日志获取详细信息。");
    }

    private static void handleStats(Scanner scanner, HDFSFileManager manager) {
        String hdfsDir = promptRequired(scanner, "请输入要统计的 HDFS 目录路径");
        if (hdfsDir == null) {
            System.out.println("已返回主菜单。");
            return;
        }
        DirectoryStats stats = manager.getDirectoryStats(hdfsDir);
        System.out.println("统计结果：" + stats);
    }

    private static void handleDelete(Scanner scanner, HDFSFileManager manager) {
        String hdfsPath = promptRequired(scanner, "请输入要删除的 HDFS 路径");
        if (hdfsPath == null) {
            System.out.println("已返回主菜单。");
            return;
        }
        Boolean recursive = promptConfirm(scanner, "若目标为目录是否递归删除？(y/N)", false);
        if (recursive == null) {
            System.out.println("已返回主菜单。");
            return;
        }
        boolean success = manager.deleteFile(hdfsPath, recursive);
        System.out.println(success ? "删除成功。" : "删除失败，请查看日志获取详细信息。");
    }

    private static String promptWithDefault(Scanner scanner, String message, String defaultValue) {
        System.out.print(message);
        String line = scanner.nextLine().trim();
        return line.isEmpty() ? defaultValue : line;
    }

    private static String promptRequired(Scanner scanner, String message) {
        while (true) {
            System.out.print(message + "（输入 back 返回主菜单）：");
            String line = scanner.nextLine().trim();
            if (line.isEmpty()) {
                System.out.println("输入不能为空，请重新输入。");
                continue;
            }
            if ("back".equalsIgnoreCase(line)) {
                return null;
            }
            return line;
        }
    }

    private static Boolean promptConfirm(Scanner scanner, String message, boolean defaultValue) {
        System.out.print(message + "（输入 back 返回主菜单）：");
        String line = scanner.nextLine().trim().toLowerCase(Locale.ROOT);
        if (line.isEmpty()) {
            return defaultValue;
        }
        if ("back".equals(line)) {
            return null;
        }
        return "y".equals(line) || "yes".equals(line);
    }

    private static void createLocalSample(String localPath) throws IOException {
        File file = new File(localPath);
        if (!file.exists()) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                writer.write("示例数据 1\n");
                writer.write("示例数据 2\n");
                writer.write("示例数据 3\n");
            }
        }
    }
}
