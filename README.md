# 原版源码

该目录的 Go 运行源码和 UI 来自 AM1 的 0.1.4-am1.1 API Key 适配版，保持原文件内容。

完整安装、编译和签名说明见 `../docs/安装说明.md`，原版已知问题见 `../docs/功能边界和已知问题.md`。

`manifest.source.json` 保留原先精确版本声明；`../package/manifest.readonly.json` 是后来扩展至定制 0.2.17 宿主的已发布清单。不要把原源码README里的历史 OAuth-only描述误认为本版本不支持 API Key。

构建依赖使用 go.mod / go.sum 固定；第三方 pluginapi 的许可证保留于 third_party/pluginapi/LICENSE。源码中 `fixture`、example.com 等内容是本地测试夹具，不是线上账号。
