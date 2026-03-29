# Git多设备错位爬取数据协作说明

## 目的

本文用于说明如何通过 Git / GitHub 在多台设备之间同步 `bilibili-data` 项目的代码、`outputs/` 目录中的爬取产物，以及 `docs/` 文档，以支持错位接力式爬取。

这里的“错位爬取”指：

- 设备 A 跑完一轮后提交并推送结果
- 设备 B 拉取最新状态后继续跑下一轮
- 两台设备不同时修改同一个进行中的任务状态

## 追踪范围

当前 `.gitignore` 已调整为允许 Git 追踪：

- `outputs/` 下全部文件
- `docs/` 下全部文件

这意味着以下内容可以被同步到 GitHub：

- `outputs/video_pool/uid_expansions/` 下的 `videolist_part_*.csv`
- `outputs/video_pool/uid_expansions/` 下的 `remaining_uids_part_*.csv`
- `uid_expansion_state.json`
- 日志文件与 summary 文件
- 其他输出目录中的 CSV、JSON、日志等结果文件

## 干净新设备首次接手的初始化步骤

下面这套流程适用于一台刚接手本项目的干净新设备，目标是让它具备：

- 正常 `pull / commit / push` 本仓库的能力
- 正常安装并运行 `bilibili-data` 所需 Python 依赖的能力
- 正常运行 `bvd-crawler.py` 并连接 BigQuery / GCS 的能力

### 1. 安装基础工具

建议至少安装以下工具：

- `Git`
- `Python 3.10+`
- `uv`
- `Google Cloud CLI`

如果是 Windows，可以优先尝试：

```powershell
winget install --id Git.Git -e
winget install --id Python.Python.3.11 -e
winget install --id astral-sh.uv -e
winget install --id Google.CloudSDK -e
```

如果新设备上已经安装好了 Python，也可以直接用 `pip` 安装 `uv`，只把 `Git` 和 `Google Cloud CLI` 交给系统包管理器安装：

```powershell
winget install --id Git.Git -e
python -m pip install -U pip
python -m pip install -U uv
winget install --id Google.CloudSDK -e
```

安装完成后，建议先检查版本：

```powershell
git --version
python --version
uv --version
gcloud --version
```

### 2. 配置 Git 提交身份

新设备第一次参与协作前，先配置本机 Git 用户名和邮箱。这里填写的是 **Git 提交身份**，通常建议与自己的 GitHub 账号保持一致：

```powershell
git config --global user.name "你的GitHub用户名或你的名字"
git config --global user.email "你的GitHub邮箱"
```

检查是否已生效：

```powershell
git config --global --get user.name
git config --global --get user.email
```

如果你通过 HTTPS 拉取私有仓库，首次 `git clone` / `git push` 时还需要完成 GitHub 认证。建议直接使用系统凭据管理器或 GitHub Personal Access Token，不要把 Token 写入仓库文件。

### 2.1 使用 GitHub Personal Access Token（PAT）进行本地认证

如果新设备没有配置 SSH 公钥，或者出现 `Permission denied (publickey)`，建议直接改用 **HTTPS + PAT** 方式访问 GitHub 私有仓库。

#### 第一步：在 GitHub 创建 PAT

在 GitHub 网页中进入：

- `Settings`
- `Developer settings`
- `Personal access tokens`
- `Tokens (classic)` 或 `Fine-grained tokens`

建议：

- 如果你只需要访问自己的私有仓库，可以优先使用 `Fine-grained token`
- 如果你想减少兼容性问题，也可以使用 `Tokens (classic)`

至少保证该 Token 具备对应仓库的读写权限。对于常见协作场景，至少要能：

- clone
- pull
- push

创建完成后，**立即复制并保存该 Token**；离开页面后通常无法再次完整查看。

#### 第二步：优先使用 HTTPS 仓库地址

不要使用下面这种 SSH 地址：

```powershell
git@github.com:<owner>/<repo>.git
```

建议使用 HTTPS 地址：

```powershell
https://github.com/<owner>/<repo>.git
```

#### 第三步：在本机启用 Git Credential Manager

Windows 下通常可以直接让 Git 使用系统凭据管理器保存 PAT：

```powershell
git config --global credential.helper manager-core
```

检查是否已生效：

```powershell
git config --global --get credential.helper
```

#### 第四步：首次 clone / pull / push 时输入 GitHub 用户名和 PAT

例如：

```powershell
git clone https://github.com/<owner>/<repo>.git
```

首次认证时：

- `Username` 输入你的 GitHub 用户名
- `Password` 位置不要填 GitHub 登录密码，而是填写刚刚创建的 `PAT`

如果 `manager-core` 已启用，Windows 会自动把这组凭据保存在系统凭据管理器中，后续通常不需要重复输入。

#### 第五步：如果之前误用了 SSH 地址，可切换远程地址

如果仓库已经克隆到本地，但远程地址还是 SSH，可以在仓库目录下改成 HTTPS：

```powershell
git remote -v
git remote set-url origin https://github.com/<owner>/<repo>.git
git remote -v
```

#### 第六步：如需清除旧的错误凭据

如果你之前输错过密码或 Token，导致后续一直认证失败，可以先在 Windows“凭据管理器”中删除旧的 GitHub 凭据，然后重新执行一次 `git pull` 或 `git push` 触发重新登录。

### 2.2 使用 GitHub SSH 密钥进行本地认证

如果你希望通过 SSH 方式访问 GitHub，例如使用：

```powershell
git@github.com:<owner>/<repo>.git
```

那么新设备需要先完成本地 SSH 密钥配置，并把公钥添加到 GitHub 账号。

#### 第一步：检查本机是否已有 SSH 密钥

先查看当前用户目录下是否已经存在 SSH 密钥：

```powershell
Get-ChildItem "$HOME\.ssh"
```

常见文件包括：

- `id_ed25519`
- `id_ed25519.pub`
- `id_rsa`
- `id_rsa.pub`

如果已经有一套你确认可用的 GitHub SSH 密钥，可以直接复用；如果没有，建议新建一套 `ed25519` 密钥。

#### 第二步：生成新的 SSH 密钥

推荐使用 GitHub 邮箱作为注释：

```powershell
ssh-keygen -t ed25519 -C "你的GitHub邮箱"
```

执行后：

- 直接回车可使用默认路径 `C:\Users\你的用户名\.ssh\id_ed25519`
- 可以按需为私钥设置 passphrase
- 如果这是个人长期使用设备，建议设置 passphrase 并妥善保管

#### 第三步：启动并启用 `ssh-agent`

Windows 上建议先确保 `ssh-agent` 服务可用：

```powershell
Get-Service ssh-agent
Set-Service -Name ssh-agent -StartupType Manual
Start-Service ssh-agent
```

然后把新生成的私钥加入 agent：

```powershell
ssh-add "$HOME\.ssh\id_ed25519"
```

如果你使用的是其他密钥文件名，请把路径替换成对应文件。

#### 第四步：复制公钥内容

把公钥打印出来：

```powershell
Get-Content "$HOME\.ssh\id_ed25519.pub"
```

或者直接复制到剪贴板：

```powershell
Get-Content "$HOME\.ssh\id_ed25519.pub" | Set-Clipboard
```

#### 第五步：将公钥添加到 GitHub

在 GitHub 网页中进入：

- `Settings`
- `SSH and GPG keys`
- `New SSH key`

然后：

- `Title` 填写这台设备的名字，例如 `Surface-Laptop-2026`
- `Key type` 选择 `Authentication Key`
- `Key` 粘贴刚刚复制的公钥内容

保存后，这台设备就获得了通过 SSH 访问 GitHub 的能力。

#### 第六步：测试 SSH 连接

执行：

```powershell
ssh -T git@github.com
```

第一次连接时，通常会提示是否信任 GitHub 主机指纹，输入：

```text
yes
```

如果配置成功，通常会看到类似“成功认证，但 GitHub 不提供 shell 访问”的提示，这表示 SSH 已经可以正常用于 Git 远程连接。

#### 第七步：使用 SSH 地址 clone 仓库

完成上面的配置后，就可以直接使用 SSH 地址：

```powershell
git clone git@github.com:<owner>/<repo>.git
```

#### 第八步：把已存在仓库的远程地址切换为 SSH

如果你已经通过 HTTPS clone 了仓库，但后续想切换为 SSH，可以在仓库目录下执行：

```powershell
git remote -v
git remote set-url origin git@github.com:<owner>/<repo>.git
git remote -v
```

#### 第九步：如果仍然出现 `Permission denied (publickey)`

请优先检查：

- 当前 GitHub 账号下是否真的添加了这台设备对应的公钥
- `ssh-add` 是否已把正确的私钥加入 agent
- 当前仓库远程地址是否确实为 `git@github.com:...`
- 本机是否加载了旧的、错误的 SSH key

必要时可以执行：

```powershell
ssh-add -l
```

查看当前 agent 中实际加载了哪些密钥。

### 3. 将项目拉取到本地

选择一个本地工作目录后执行：

```powershell
cd "D:\Schoolworks"
git clone https://github.com/<owner>/<repo>.git
cd ".\Thesis\bilibili-data"
```

如果设备上已经有仓库目录，但不是最新状态，则进入项目目录后执行：

```powershell
git pull --rebase
```

### 4. 安装项目依赖

本项目的 Python 依赖定义在 `pyproject.toml` 中，新设备拉取代码后，推荐直接执行：

```powershell
uv sync
```

如果你只想临时运行，也可以使用：

```powershell
uv run streamlit run .\bvd-crawler.py
```

但对于长期协作设备，仍建议先执行一次 `uv sync`，保证依赖环境稳定。

### 5. 登录并初始化 GCP

由于 `bvd-crawler` 需要访问 BigQuery 和 GCS，新设备还需要完成本机 GCP 登录：

```powershell
gcloud auth login
gcloud auth application-default login
```

然后设置当前项目：

```powershell
$PROJECT_ID = "你的GCP项目ID"
gcloud config set project $PROJECT_ID
```

说明：

- `gcloud auth login` 主要用于 CLI 管理操作
- `gcloud auth application-default login` 会为本机生成 ADC
- 即使后续改用服务账号 JSON，本机先完成一次 GCP 登录通常也更方便排查问题

### 6. 确认或创建 `SA_NAME`

对于 `bvd-crawler`，推荐使用 **普通服务账号 + 本地 JSON 密钥文件**，不要使用 Cloud Storage 的 `service agent`。

建议统一约定：

```powershell
$PROJECT_ID = "你的GCP项目ID"
$REGION = "us-central1"
$BQ_DATASET = "bili_video_data_crawler"
$GCS_BUCKET = "你的GCS Bucket名称"
$SA_NAME = "bvd-crawler-sa"
$SA_EMAIL = "$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com"
```

先查看项目里是否已经存在该服务账号：

```powershell
gcloud iam service-accounts list
```

如果已经存在，就直接复用；如果不存在，再创建：

```powershell
gcloud iam service-accounts create $SA_NAME --display-name="BVD Crawler Service Account"
```

### 7. 给服务账号授权

如果这是新创建的服务账号，需要至少确保它有访问 BigQuery 和 GCS 的权限。为了先跑通，建议直接授予：

```powershell
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA_EMAIL" --role="roles/bigquery.admin"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA_EMAIL" --role="roles/storage.admin"
```

如果项目里已经提前建好了 BigQuery Dataset 和 Bucket，也可以后续再按需收紧为更小权限；但在多设备协作初期，先保证可用性通常更重要。

### 8. 在本地创建服务账号 JSON 密钥文件

推荐把本地私有配置统一放在仓库下的 `.local/` 目录中；该目录已经被 `.gitignore` 忽略，不会被提交到 GitHub。

先创建目录：

```powershell
New-Item -ItemType Directory -Force ".\.local\gcp" | Out-Null
```

然后把该服务账号的 key 生成为本地 JSON 文件：

```powershell
gcloud iam service-accounts keys create ".\.local\gcp\bvd-crawler-sa.json" --iam-account=$SA_EMAIL
```

注意事项：

- 这一步生成的是 **服务账号私钥**，只应保存在当前设备本地
- 不要把 `.json` 密钥复制到 `docs/`、`outputs/` 或任何会被 Git 跟踪的目录
- 如果设备更换、遗失或不再使用，建议及时在 GCP 中吊销对应 key

### 9. 准备 BigQuery Dataset 和 GCS Bucket

`bvd-crawler` 运行时会自动尝试创建 BigQuery Dataset 和相关表，但 **不会自动创建 GCS Bucket**。因此新设备至少要确认下面两项：

- `BigQuery Dataset` 名称已确定，例如 `bili_video_data_crawler`
- `GCS Bucket` 已存在，例如 `your-bilibili-media-bucket`

如果 Bucket 还没建，可以创建：

```powershell
gcloud storage buckets create "gs://$GCS_BUCKET" --location=$REGION --uniform-bucket-level-access
```

### 10. 在 `bvd-crawler` 中填写本地 GCP 配置

启动应用：

```powershell
uv run streamlit run .\bvd-crawler.py
```

然后在侧边栏的 Google Cloud 配置区填写：

- `GCP Project ID`：`$PROJECT_ID`
- `BigQuery Dataset`：`$BQ_DATASET`
- `GCS Bucket 名称`：`$GCS_BUCKET`
- `GCP Region`：`$REGION`
- `服务账号 JSON 路径`：`.local\gcp\bvd-crawler-sa.json`
- `GCS 对象前缀`：建议保留默认 `bilibili-media`

填完后建议：

- 点击“保存配置”，让应用把本机配置写入 `.local/bvd-crawler.gcp.config.json`
- 点击“测试 GCP 连接”，确认 BigQuery 和 GCS 都可访问

### 11. 新设备首次接手前的最小验证

推荐按下面顺序做一次最小验证：

```powershell
git pull --rebase
uv sync
uv run streamlit run .\bvd-crawler.py
```

在应用里完成一次 “测试 GCP 连接” 后，再做一次小规模抓取测试；确认正常后，这台设备就可以正式加入错位接力协作。

## 推荐协作原则

### 1. 同一时刻只允许一台设备继续某个任务

尤其是 `uid_expansion` 任务，同一个 session 下会同时写入：

- `uid_expansion_state.json`
- `remaining_uids_part_*.csv`
- `videolist_part_*.csv`
- `logs/`

如果两台设备同时继续同一个 session，极易出现文件内容分叉，导致：

- state 被覆盖
- `remaining_uids` 对不上
- 同一 part 编号在不同设备上产生不同结果
- Git merge 冲突难以人工判断

### 2. 每次开始爬取前先同步

在任意设备开始新一轮爬取前，先执行：

```powershell
git pull --rebase
```

确保本地拿到最新的：

- 代码逻辑
- `outputs/` 中的任务状态
- 上一台设备刚产生的剩余 UID 文件和导出结果

### 3. 每次结束后立刻提交并推送

一轮任务结束后，尽快执行：

```powershell
git add .
git commit -m "sync crawl outputs after device run"
git push
```

这样另一台设备接手时可以直接在最新状态上续跑。

## 推荐操作流程

## 场景 A：设备 A 跑完，设备 B 接着跑

### 设备 A

1. 先同步仓库

```powershell
git pull --rebase
```

2. 运行一次爬取任务

3. 确认 `outputs/` 中已生成最新文件，例如：

- `videolist_part_n.csv`
- `remaining_uids_part_n.csv`
- `uid_expansion_state.json`
- 对应日志文件

4. 提交并推送

```powershell
git add .
git commit -m "sync uid expansion outputs after part n"
git push
```

### 设备 B

1. 拉取最新状态

```powershell
git pull --rebase
```

2. 打开 DataHub 或相关脚本

3. 使用刚同步下来的文件继续：

- 如果是中断续跑，优先使用 `remaining_uids_part_n.csv`
- 如果是重新补抓整批作者，使用 `original_uids.csv` 新开 session

4. 跑完后继续提交并推送

```powershell
git add .
git commit -m "continue uid expansion on device B"
git push
```

## `uid_expansion` 的建议做法

### 中断续跑

如果你只是接着上一次未完成的 session 继续跑：

- 上传对应 session 中最新的 `remaining_uids_part_n.csv`
- 继续生成新的 `part_(n+1)`
- 跑完后提交 `outputs/` 变化

这是最适合双设备接力的方式。

### 重新补抓

如果你想让一批作者重新按新的窗口规则补抓：

- 使用该 session 的 `original_uids.csv`
- 创建一个新的 `uid_expansion` session
- 跑完后再提交

这种方式适合：

- 修复了时间窗口逻辑后重新补数据
- 想从更早日期重新覆盖抓取
- 不希望污染旧 session 的续跑链路

## 提交信息建议

建议把“代码改动”和“数据同步”分开提交。

### 代码改动

```powershell
git add bvp-builder.py src/ docs/
git commit -m "fix uid expansion history start resolution"
```

### 数据同步

```powershell
git add outputs/
git commit -m "sync outputs after crawl on laptop"
```

这样后续回看历史时更容易分辨：

- 哪次提交改了逻辑
- 哪次提交只是同步了爬取结果

## 冲突处理建议

如果 `git pull --rebase` 时出现冲突，优先检查这些文件：

- `outputs/video_pool/uid_expansions/*/uid_expansion_state.json`
- `outputs/video_pool/uid_expansions/*/remaining_uids_part_*.csv`
- `outputs/video_pool/uid_expansions/*/videolist_part_*.csv`
- 各类 `.log`

处理原则：

- 如果只有一台设备在继续某个 session，理论上不应频繁冲突
- 如果发生冲突，通常说明两台设备曾同时改动同一任务
- 这时应先停下，人工确认哪台设备的结果应保留

## 仓库体积注意事项

由于现在 `outputs/` 全部纳入版本管理，仓库体积会持续增长。请注意：

- CSV 和日志文件会快速增大仓库历史
- 拉取和推送速度会逐渐变慢
- 单文件过大时可能接近 GitHub 限制

如果后续 `outputs/` 体积明显变大，建议考虑：

- 定期归档旧任务目录
- 拆分数据仓库与代码仓库
- 使用 Git LFS 管理超大文件

## 最简执行模板

每次切换设备时，按下面顺序执行即可：

### 接手前

```powershell
git pull --rebase
```

### 爬取后

```powershell
git add .
git commit -m "sync outputs after crawl"
git push
```

## 一句话规则

可以把这套协作方式记成一句话：

**先拉取，再爬取；爬完立即提交推送；同一个 session 不双机并发。**
