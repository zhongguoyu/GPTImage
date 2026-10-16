"use client";
import { useEffect, useState } from "react";
import { httpRequest } from "@/lib/request";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

type Routing = { mode: "native" | "sub2api"; explicit: boolean; image_upstream: boolean; account_refresh?: {refreshed?: number; failed?: number; error?: string} };
export function RoutingModeCard() {
  const [current, setCurrent] = useState<Routing | null>(null);
  const [selected, setSelected] = useState<Routing["mode"]>("native");
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  useEffect(() => {
    httpRequest<Routing>("/api/routing-mode").then(value => {
      setCurrent(value); setSelected(value.mode);
    }).catch(error => setMessage(String(error.message || error)));
  }, []);
  async function save() {
    setBusy(true); setMessage("");
    try {
      const value = await httpRequest<Routing>("/api/routing-mode", {method: "POST", body: {mode: selected}});
      setCurrent(value);
      const sync = value.account_refresh;
      setMessage(`链路已切换。${sync?.error || `账号同步成功 ${sync?.refreshed ?? 0} 个，失败 ${sync?.failed ?? 0} 个。`}${sync?.failed ? "未同步账号显示待刷新，请到号池管理重试。" : ""}请重新读取模型列表。`);
    } catch (error) { setMessage(error instanceof Error ? error.message : "切换失败"); }
    finally { setBusy(false); }
  }
  return <Card className="mb-4">
    <CardHeader><CardTitle>上游链路</CardTitle></CardHeader>
    <CardContent className="space-y-4">
      <p className="text-sm text-muted-foreground">统一控制文本、生图、编辑、模型列表、搜索及账号刷新。无限画布随图片接口一起切换，两个方案的配置都会保留。</p>
      <div className="flex flex-wrap gap-3" role="group" aria-label="选择上游链路">
        <Button variant={selected === "native" ? "default" : "outline"} disabled={busy || !current} aria-pressed={selected === "native"} onClick={() => setSelected("native")}>ChatGPT2API 原生</Button>
        <Button variant={selected === "sub2api" ? "default" : "outline"} disabled={busy || !current} aria-pressed={selected === "sub2api"} onClick={() => setSelected("sub2api")}>Sub2API 网关</Button>
      </div>
      <p className="text-sm">当前：{current ? current.mode === "native" ? "ChatGPT2API 原生" : "Sub2API 网关" : "读取中"}</p>
      {current && !current.explicit && <p className="text-xs text-muted-foreground">尚未设置统一开关，当前沿用部署环境配置；保存后统一生效。</p>}
      <p className="text-xs text-muted-foreground">原生方案使用原账号池与协议实现；网关方案使用已配置的 Sub2API。模型和账号权限以对应上游为准，失败不会自动切换。请等当前任务完成后切换。</p>
      {message && <p role="status" className="text-sm">{message}</p>}
      <Button disabled={busy || !current || (current.explicit && current.mode === selected)} onClick={() => void save()}>{busy ? "切换并同步账号中…" : "保存并切换"}</Button>
    </CardContent>
  </Card>;
}
