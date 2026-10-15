# GitHub 发布

仓库已按源码发布整理；不包含生产环境、凭证或 Git 历史。发布前检查当前工作树，尤其是你后来新增的配置。

## 检查和首次提交

```bash
python -m unittest discover -s tests -v
git add .
python scripts/check_release.py
git diff --cached --stat
git status --short
git commit -m "Initial GPTImage integration"
```

脚本不会打印匹配到的秘密值，只报告文件和规则。自动扫描不是没有秘密的数学证明；仍须人工查看准备提交的文件。不要强制加入 `.env`、`runtime/` 或备份。实际账号信息属于部署数据，不应使用 Git 管理。

然后在 GitHub 创建空仓库，按页面提供的远程 URL 推送。例如：

```bash
git branch -M main
git remote add origin https://github.com/YOUR_ACCOUNT/GPTImage.git
git push -u origin main
```

仓库名称和账号由你选择。这里的命令是发布步骤，整理项目不会自动创建或推送远程仓库。

## 许可证与来源

保留根目录说明、两套上游 LICENSE、源码文件版权声明，以及 `upstream-lock.json`。不要把两套第三方代码全部声明成自己的 MIT 项目。Sub2API 的 LGPL 源码和许可证必须随相应分发方式保持可获得；详见原许可证。根目录 MIT 只覆盖本项目新增集成材料及适用的修改。

## 验证范围

集成行为已在原运行环境验证：图片转发、图片编辑、模型名称透传、账号状态同步、默认无限制额度、图库保存；图库修复有 16 项相关回归通过，额度变更有 27 项相关回归通过。

本仓库额外验证记录写入 `docs/VALIDATION.md`。原环境的成功不能代替新部署验收；完整的新安装、Sub2API 源码编译和你的账号/网络可用性应在目标环境验证。
