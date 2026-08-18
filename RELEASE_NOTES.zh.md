# 本发行来源分支：`feat/ferry-ingest`

这是 **coloraven** 对 [artifact-keeper/artifact-keeper](https://github.com/artifact-keeper/artifact-keeper) 的**分叉发行**，不是上游官方 Release。

- **来源分支（显著）：`feat/ferry-ingest`**
- **对照基准：** fork 时与上游的 merge-base `b0507587`（`fix(security): build grype from source…`）
- **目标运行时：** Linux AMD64 + UBI9 类容器（须在 UBI9 内编译，避免 glibc 2.38+ 无法运行）

---

## 相对 fork 基准新增的功能

### 空气隔离摆渡（Ferry ingest）

在「外网打包 → 介质拷贝 → 内网入库」路径上，服务端配套 CLI 的 ferry zip / 目录摆渡：

- 识别 `ak-ferry/*.zip`，**流式**按 zip 条目入库（Go / npm / PyPI / Cargo），避免 TB 级整包先落盘再扫。
- 分片上传（chunked complete）成功后**自动启动** ingest；也可手动 `POST .../ferry/ingest`。
- 提供 ingest job 进度查询（queued / running / completed / failed / partial）。
- 可选 ingest 成功后回收 ferry zip（环境变量控制）。
- 重入时按 **checksum** 跳过已在库构件；已发布坐标若字节不同则记冲突，不再静默覆盖。Go 的 `.zip` / `.mod` 分别判断，缺一补一。

### 断点续传（服务端配套 CLI）

CLI 已支持分片 session 本地缓存、下载 Range、skip-dupe。本分叉服务端对齐：

- `PUT /api/v1/uploads/{id}/complete` 失败时：**保留临时文件**、释放 committing lease，客户端可对同一 session 重试 complete。
- 不可变（已发布）坐标写入不同字节 → **HTTP 409** `Artifact version already exists and is immutable`（与 `ak artifact push --skip-dupe-uploads` 一致）。
- 同 checksum 重传视为幂等续传，允许 complete 走完。
- `GET /api/v1/uploads/{id}` 增加 `error_message`，便于判断 session 是否仍可续。

### 构建与部署

- GitHub Actions：`feat/**` / `fix/**` push 时在 **UBI9** 容器内编译 `artifact-keeper-linux-amd64`。
- 禁止在磁盘不足的本机或低配部署机上 `cargo` 编译；一律用 GitHub Actions 出包。

### 未包含（相对上游完整 release）

本 workflow **不**发布 GHCR/Docker Hub 镜像、不跑上游 `artifact-keeper-test` release-gate。二进制矩阵对齐上游：linux-amd64（UBI9）、linux-arm64、darwin-amd64、darwin-arm64、windows-amd64。
