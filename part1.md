## 第一部分：基础操作评估（5 分）

### 任务 1：目录和文件管理

**任务描述**：使用 HDFS 命令行工具完成以下操作，并通过 NameNode Web UI 确认结果。

#### 1.1 目录结构创建

```bash
# 要求：创建以下目录结构：
# /user/student/project/
# ├── input/
# ├── output/
# └── temp/
```

- 命令执行截图：
![alt text](image.png)

**Web UI 验证要求**：
- 在 NameNode Web UI 中确认目录结构创建成功。
![alt text](image-1.png)

#### 1.2 文件上传和管理

```bash
# 要求：
# 1. 在本地创建一个测试文件 test.txt（包含至少 100 行数据）
# 2. 上传至 /user/student/project/input/ 目录
# 3. 查看文件内容的前 10 行和后 10 行
# 4. 查看文件的详细属性信息
```

- 命令执行截图：
![alt text](image-2.png)

**Web UI 验证要求**：
- 在 Web UI 中确认文件上传成功。
![alt text](image-3.png)

#### 1.3 文件操作和权限管理

```bash
# 要求：
# 1. 将 input 目录中的文件复制到 temp 目录
# 2. 修改 temp 目录中文件的权限为 644
# 3. 修改 temp 目录的权限为 755
# 4. 验证权限设置是否正确
```

- 命令执行截图：
![alt text](image-4.png)

**Web UI 验证要求**：
- 在 Web UI 中确认文件复制和权限设置成功。
- 目录权限截图：![alt text](image-5.png)
- 文件权限截图：![alt text](image-6.png)

### 任务 2：批量操作

**任务描述**：使用 HDFS 命令行工具完成批量文件操作。

#### 2.1 批量文件上传

```bash
# 要求：
# 1. 在本地创建 5 个不同的文件（file1.txt 至 file5.txt）
# 2. 批量上传这些文件到 /user/student/project/input/
# 3. 使用通配符验证所有文件都已上传成功
```

- 命令执行截图：
![alt text](image-7.png)

**Web UI 验证要求**：
- 在 Web UI 中确认所有文件都已批量上传成功。
![alt text](image-8.png)

#### 2.2 通配符操作

```bash
# 要求：
# 1. 使用通配符列出所有 .txt 文件
# 2. 使用通配符复制所有以 "file" 开头的文件到 temp 目录
# 3. 统计 input 目录中文件的总数
```

- 命令执行截图：
![alt text](image-9.png)

**Web UI 验证要求**：
- 在 Web UI 中确认通配符操作结果正确。
![alt text](image-10.png)

#### 2.3 目录操作和清理

```bash
# 要求：
# 1. 创建一个备份目录 /user/student/backup/
# 2. 将整个 project 目录复制到 backup 目录
# 3. 删除 temp 目录中的所有文件（保留目录）
# 4. 验证操作结果
```

- 命令执行截图：
![alt text](image-11.png)

**Web UI 验证要求**：
- 在 Web UI 中确认备份和清理操作成功。
- 备份结果截图：![alt text](image-13.png)
- temp 目录清理截图：![alt text](image-12.png)

---