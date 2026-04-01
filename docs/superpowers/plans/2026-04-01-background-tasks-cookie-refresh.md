# Background Tasks And Cookie Refresh Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 将 DataHub 中两类长任务改为后台子进程执行，并让任务按批次重读本地 cookie 文件，以支持运行期热更新 cookie 而不中断任务。

**Architecture:** 新增一个轻量后台任务层：前端负责写任务配置、启动子进程、轮询状态与更新 cookie 文件；子进程负责运行具体任务、持续写状态 JSON 和日志。cookie 不再只作为前端内存中的 `Credential` 传入，而是以本地文件为真源，由批处理执行器在每一批开始前读取一次，并在批内遇到认证/风控类错误时可即时刷新后继续处理剩余项目。

**Tech Stack:** Python, Streamlit, subprocess, json, unittest

---

### Task 1: 写后台任务与 cookie 批次刷新失败测试

**Files:**
- Modify: `tests/test_manual_batch_runner.py`
- Modify: `tests/test_manual_media_runner.py`
- Create: `tests/test_datahub_background_tasks.py`

**Step 1: Write the failing test**
- 为批次 cookie 刷新新增测试，验证批次执行器在每批前调用 cookie 提供函数。
- 为后台任务配置/状态读写新增测试，验证任务配置可落盘、状态可更新、任务目录可解析。
- 为 worker 入口新增测试，验证可根据任务类型调度到 manual batch / manual media 入口。

**Step 2: Run test to verify it fails**
- Run: `python -m pytest tests/test_manual_batch_runner.py tests/test_manual_media_runner.py tests/test_datahub_background_tasks.py -q`
- Expected: 旧实现缺少后台任务与 cookie 批次刷新抽象，测试失败。

### Task 2: 实现批次 cookie 刷新抽象

**Files:**
- Modify: `src/bili_pipeline/datahub/manual_batch_runner.py`
- Modify: `src/bili_pipeline/datahub/manual_media_runner.py`
- Create: `src/bili_pipeline/datahub/background_tasks.py`

**Step 1: Add cookie provider support**
- 在 runner 层增加可选 `credential_provider` / `batch_size` 参数。
- 将待抓 CSV 按批次切分；每批开始前读取一次 cookie。
- 批内复用同一个 `Credential`；若批次执行结果出现疑似认证/风控类错误且存在剩余 CSV，则允许在进入下一批前再次重读 cookie。

**Step 2: Keep existing behavior compatible**
- 若未提供 provider，则继续沿用当前直接传入 `credential` 的旧路径。

### Task 3: 实现后台子进程任务层

**Files:**
- Create: `src/bili_pipeline/datahub/background_tasks.py`
- Create: `scripts/datahub_background_worker.py`

**Step 1: Create persistent task metadata**
- 定义任务配置、任务状态、cookie 文件路径、pid、活跃任务锁文件等结构。

**Step 2: Create worker dispatch**
- 支持任务类型：
  - `manual_dynamic_batch`
  - `manual_media_mode_a`
  - `manual_media_mode_b`
- worker 从 JSON 配置读取参数，运行对应任务，并持续更新状态文件。

### Task 4: 更新 DataHub 前端

**Files:**
- Modify: `bilibili-datahub.py`
- Modify: `src/bili_pipeline/datahub/shared.py`

**Step 1: Persist cookie**
- 新增本地 cookie 文件读写。
- 侧边栏提供“保存/更新 cookie”按钮和当前状态提示。

**Step 2: Launch and monitor background tasks**
- 两类长任务页面改为：
  - 创建任务
  - 启动后台 worker
  - 读取状态/日志
  - 停止任务（如果本轮实现可安全支持）

### Task 5: 更新说明文档

**Files:**
- Modify: `Bilibili_DataHub.md`

**Step 1: Update docs**
- 补充后台任务运行方式、cookie 文件行为、页面重跑/关闭页签后任务继续的说明。

### Task 6: 验证

**Files:**
- Modify: `tests/test_manual_batch_runner.py`
- Modify: `tests/test_manual_media_runner.py`
- Create: `tests/test_datahub_background_tasks.py`

**Step 1: Run focused tests**
- Run: `python -m pytest tests/test_manual_batch_runner.py tests/test_manual_media_runner.py tests/test_datahub_background_tasks.py -q`
- Run: `python -m pytest tests/test_realtime_watchlist_runner.py -q`

**Step 2: Lint check**
- 对修改文件运行诊断检查，确认无新增错误。
