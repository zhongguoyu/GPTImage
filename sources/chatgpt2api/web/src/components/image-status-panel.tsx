"use client";
import { useEffect, useState } from "react";
import { fetchImageOperationalStatus, type ImageOperationalStatus } from "@/lib/api";
function at(value: number | null) {
  if (!value) return "暂无";
  return new Date(value * 1000).toLocaleString("zh-CN", {timeZone:"Asia/Shanghai", month:"2-digit",day:"2-digit",hour:"2-digit",minute:"2-digit"});
}
export function ImageStatusPanel({model = "gpt-image-2"}: {model?: string}) {
  const [data,setData]=useState<ImageOperationalStatus | null>(null);
  const [error,setError]=useState(false);
  useEffect(()=>{
    let stopped=false;
    let busy=false;
    setData(null);setError(false);
    const load=async()=>{
      if (busy) return;
      busy=true;
      try { const result=await fetchImageOperationalStatus(model); if(!stopped){setData(result);setError(false);} }
      catch {if(!stopped)setError(true);}
      finally {busy=false;}
    };
    void load();const timer=setInterval(()=>void load(),30000);
    return ()=>{stopped=true;clearInterval(timer);};
  },[model]);
  const pool=data?.pool;
  let title="待验证";
  let color="text-stone-600";
  if(error) title="状态暂时无法获取";
  else if(!data) title="正在读取生图状态";
  else if(!pool) title="账户池状态未知";
  else if(pool.total===0) title="没有可用账户";
  else if(pool.available===0) title=pool.cooling>0 ? "账户冷却中" : "暂无可调度账户";
  else if(data.state==="failed"){title=data.consecutive_failures>1 ? "连续失败 "+data.consecutive_failures+" 次" : "最近请求失败";color="text-amber-700";}
  else if(data.state==="success"){title="最近生图成功";color="text-emerald-700";}
  return <section className="console-status-panel shrink-0 rounded-sm border border-stone-200 bg-white/95 px-4 py-3 text-xs shadow-sm" aria-label="生图状态">
    <div className="flex flex-wrap items-center justify-between gap-2">
      <strong className={"text-sm "+color}>{title}</strong>
      <span className="text-stone-500">{error ? "读取失败，原统计可能已过期" : data ? "更新于 "+at(data.updated_at)+" · 每 30 秒刷新" : "加载中"}</span>
    </div>
    {data && !error && <>
      <div className="console-status-metrics mt-2 text-stone-700">
        <span>可调度 <b>{pool?.available ?? "未知"}</b> / {pool?.total ?? "未知"} · 冷却 {pool?.cooling ?? "未知"}</span>
        <span>今日成功 <b>{data.today.generate+data.today.edit}</b> 张 <span className="text-stone-400">（生成 {data.today.generate} / 编辑 {data.today.edit}）</span></span>
        <span>近 {data.recent_count} 次成功率 <b>{data.success_rate===null ? "暂无" : data.success_rate+"%"}</b></span>
        <span>任务耗时中位数 <b>{data.median_seconds===null ? "暂无" : data.median_seconds+" 秒"}</b></span>
      </div>
      <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-stone-500">
        <span>最近成功：{at(data.last_success)}</span>
        <span>剩余可生图张数：上游未提供</span>
        {data.last_error && <span className="text-amber-700">{data.last_error}</span>}
      </div>
      <details className="mt-2 text-stone-500">
        <summary className="cursor-pointer">账户用量与统计口径</summary>
        <div className="mt-2 max-h-40 space-y-2 overflow-y-auto">
          <p>{data.scope}。今日按北京时间；成功率取过去 24 小时内最近 20 个已结束任务，不含排队中任务。</p>
          <p>样本时间：{at(data.recent_from)} — {at(data.recent_to)}；耗时取其中 {data.duration_samples} 个成功任务，包含排队，历史时间精度约 1 秒。超过 30 分钟无已结束任务显示“待验证”。账户池按 {model} 判断。</p>
          {data.pool_error && <p>{data.pool_error}</p>}
          {pool?.accounts.map(a=><div key={a.label} className="rounded-lg bg-stone-50 p-2">
            <b>{a.label}</b> · {a.available ? "可调度" : a.cooling ? "冷却中" : "不可调度"}
            {a.cooldown_until && <p className="text-amber-700">Sub2API 记录恢复时间：{at(a.cooldown_until)}。来源未保留，可能为程序冷却；不代表官方额度重置。</p>}
            <p>Codex 用量：{a.windows.length ? a.windows.map(w=>w.label+(w.stale ? "窗口已到重置时间，等待更新" : "窗口剩余 "+w.remaining+"%"+(w.reset_at ? "，重置 "+at(w.reset_at) : ""))).join("；") : "暂无数据"}。不能换算成图片张数。</p>
            <p>用量快照：{a.usage_updated_at ? new Date(a.usage_updated_at).toLocaleString("zh-CN",{timeZone:"Asia/Shanghai"}) : "更新时间未知"}</p>
          </div>)}
        </div>
      </details>
    </>}
  </section>;
}
