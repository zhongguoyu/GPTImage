# 第三方来源与许可证

本仓库包含多个独立组件。各目录的原许可证和版权声明继续有效。

| 路径 | 上游 | 许可证 | 修改情况 |
|---|---|---|---|
| `sources/chatgpt2api` | https://github.com/iamdurant/chatgpt2api-personal | MIT，见目录 LICENSE | 含集成补丁 |
| `sources/sub2api` | https://github.com/Wei-Shaw/sub2api | LGPL-3.0，见目录 LICENSE | 上游源码快照，移除内置 Google OAuth 凭据 |
| 根目录脚本、部署文件、本文档 | GPTImage integration contributors | MIT，见根目录 LICENSE | 新增 |

具体 commit 和运行镜像 digest 记录在 `upstream-lock.json`。ChatGPT2API 原作者版权声明保留在源码 LICENSE 中；原项目及 fork 的历史来源见其 README，不冒充原作者。

Sub2API 随附的 LGPL v3 文本引用 GPL v3，补充文本位于 `licenses/GPL-3.0.txt`。本集成通过 HTTP 调用两个独立应用，不因此删除或替换任一组件的许可。第三方依赖遵循各自许可证，不由根目录 MIT 统一覆盖。

## 可选 Infinite Canvas

`compose.canvas.yaml` 引用 [basketikun/infinite-canvas](https://github.com/basketikun/infinite-canvas) 官方容器镜像，固定版本和 digest 见 `upstream-lock.json`，许可证 MIT（Copyright (c) 2026 basketikun），副本见 `licenses/infinite-canvas-LICENSE.txt`。仓库未内嵌该组件源码或镜像字节；源码可按锁定 commit 获取。保留原作者归属，不将其声明为本项目原创。
