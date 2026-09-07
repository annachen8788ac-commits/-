# Huangshu 4K AI Video Gateway

这是《皇叔借点功德，王妃把符画猛了》AI 真人短剧的生成控制层。

## 画质策略

- 目标画幅：9:16
- Wan 2.2 生成目标：720×1280（模型支持时使用更高原生分辨率）
- 最终输出：2160×3840（4K 竖屏）
- 建议帧率：24 fps；动作镜头可插帧到 48/60 fps
- 建议编码：H.265 / 10-bit / 35 Mbps 以上
- 处理顺序：生成 → 人脸/时序修复 → AI 视频超分 → 降噪/锐化 → 4K 高码率导出

## 架构

Railway 负责运行本仓库的 Node 网关；真正的视频模型由 `COMFYUI_URL` 指向的 GPU ComfyUI 实例负责。这样可以把控制层和算力层分开，后续可以自由更换本地 GPU、云 GPU、Wan 2.2、其他视频模型或超分工作流。

## API

- `GET /health`：检查服务和 4K profile
- `GET /api/quality`：返回锁定画质参数
- `POST /api/generate`：提交 ComfyUI API-format workflow
- `GET /api/jobs/:id`：查询任务状态与输出地址
- `POST /mcp`：MCP endpoint，供 ChatGPT/Agent 调用

### 提交示例

```json
{
  "title": "EP01 Shot 01",
  "workflow": { "...": "ComfyUI API workflow JSON" }
}
```

## 环境变量

```bash
COMFYUI_URL=https://your-gpu-comfyui.example.com
PORT=3000
```

`COMFYUI_URL` 必须指向一个能够接收 `/prompt`、`/history/:id`、`/view` 的 ComfyUI 服务。

## 推荐本地免费算力路线

如果你自己有 NVIDIA GPU，可以在本机运行 ComfyUI + Wan 2.2。模型本身可开源运行，调用端不按次收费；主要成本是你自己的显卡、电费与时间。4K 视频超分非常吃显存，短镜头逐条生成与超分最稳。

## 生产建议

不要直接生成 4K 长视频。按 5–8 秒镜头生成，在角色一致性确认后逐镜头超分到 4K，再拼接成整集，可以显著降低换脸、手部异常、时序闪烁和显存压力。
