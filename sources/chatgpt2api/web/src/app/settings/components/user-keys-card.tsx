"use client";

import { useEffect, useRef, useState } from "react";
import { Ban, CheckCircle2, Copy, KeyRound, LoaderCircle, Pencil, Plus, Trash2 } from "lucide-react";
import { toast } from "sonner";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { createUserKey, deleteUserKey, fetchUserKeys, updateUserKey, type UserKey } from "@/lib/api";

function formatDateTime(value?: string | null) {
  if (!value) {
    return "—";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function parseQuotaLimit(value: string) {
  const normalized = value.trim();
  if (!/^\d+$/.test(normalized)) {
    return null;
  }
  const parsed = Number(normalized);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
}

function parseConcurrencyLimit(value: string) {
  const normalized = value.trim();
  if (!/^\d+$/.test(normalized)) {
    return null;
  }
  const parsed = Number(normalized);
  return Number.isSafeInteger(parsed) && parsed >= 1 ? parsed : null;
}

function QuotaSummary({
  label,
  used,
  reserved,
  remaining,
  unlimited,
  limit,
}: {
  label: string;
  used: number;
  reserved: number;
  remaining: number | null;
  unlimited: boolean;
  limit: number;
}) {
  return (
    <div className="min-w-0 text-xs text-stone-500">
      <div className="flex flex-wrap items-baseline gap-x-2 gap-y-1">
        <span className="font-medium text-stone-700">{label}</span>
        <span>
          已用 {used} / {unlimited ? "无限制" : limit}
        </span>
      </div>
      <div className="mt-1">{unlimited ? "无限制" : `剩余 ${remaining}`} · 处理中 {reserved}</div>
    </div>
  );
}

function ConcurrencySummary({ label, limit }: { label: string; limit: number }) {
  return (
    <div className="min-w-0 text-xs text-stone-500">
      <div className="font-medium text-stone-700">{label}</div>
      <div className="mt-1">上限 {limit} 个同时请求</div>
    </div>
  );
}

export function UserKeysCard() {
  const didLoadRef = useRef(false);
  const [items, setItems] = useState<UserKey[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [isDialogOpen, setIsDialogOpen] = useState(false);
  const [name, setName] = useState("");
  const [isCreating, setIsCreating] = useState(false);
  const [pendingIds, setPendingIds] = useState<Set<string>>(() => new Set());
  const [revealedKey, setRevealedKey] = useState("");
  const [deletingItem, setDeletingItem] = useState<UserKey | null>(null);
  const [editingItem, setEditingItem] = useState<UserKey | null>(null);
  const [editName, setEditName] = useState("");
  const [editKey, setEditKey] = useState("");
  const [createTextLimit, setCreateTextLimit] = useState("0");
  const [createTextUnlimited, setCreateTextUnlimited] = useState(true);
  const [createImageLimit, setCreateImageLimit] = useState("0");
  const [createImageUnlimited, setCreateImageUnlimited] = useState(true);
  const [createTextConcurrency, setCreateTextConcurrency] = useState("4");
  const [createImageConcurrency, setCreateImageConcurrency] = useState("1");
  const [editTextLimit, setEditTextLimit] = useState("0");
  const [editTextUnlimited, setEditTextUnlimited] = useState(false);
  const [editImageLimit, setEditImageLimit] = useState("0");
  const [editImageUnlimited, setEditImageUnlimited] = useState(false);
  const [editTextConcurrency, setEditTextConcurrency] = useState("4");
  const [editImageConcurrency, setEditImageConcurrency] = useState("1");
  const [editWebLoginEnabled, setEditWebLoginEnabled] = useState(true);

  const load = async (silent = false) => {
    if (!silent) {
      setIsLoading(true);
    }
    try {
      const data = await fetchUserKeys();
      setItems(data.items);
    } catch (error) {
      if (!silent) {
        toast.error(error instanceof Error ? error.message : "加载用户密钥失败");
      }
    } finally {
      if (!silent) {
        setIsLoading(false);
      }
    }
  };

  useEffect(() => {
    if (didLoadRef.current) {
      return;
    }
    didLoadRef.current = true;
    void load();
  }, []);

  useEffect(() => {
    let intervalId: number | undefined;

    const stopPolling = () => {
      if (intervalId !== undefined) {
        window.clearInterval(intervalId);
        intervalId = undefined;
      }
    };

    const startPolling = () => {
      stopPolling();
      if (document.visibilityState !== "visible") {
        return;
      }
      intervalId = window.setInterval(() => void load(true), 5000);
    };

    const handleVisibilityChange = () => {
      if (document.visibilityState === "visible") {
        void load(true);
        startPolling();
      } else {
        stopPolling();
      }
    };

    startPolling();
    document.addEventListener("visibilitychange", handleVisibilityChange);
    return () => {
      stopPolling();
      document.removeEventListener("visibilitychange", handleVisibilityChange);
    };
  }, []);

  const handleCreate = async () => {
    const textLimit = createTextUnlimited ? 0 : parseQuotaLimit(createTextLimit);
    const imageLimit = createImageUnlimited ? 0 : parseQuotaLimit(createImageLimit);
    const textConcurrency = parseConcurrencyLimit(createTextConcurrency);
    const imageConcurrency = parseConcurrencyLimit(createImageConcurrency);
    if (textLimit === null || imageLimit === null) {
      toast.error("额度必须是大于等于 0 的整数");
      return;
    }
    if (textConcurrency === null || imageConcurrency === null) {
      toast.error("并发必须是大于等于 1 的整数");
      return;
    }
    setIsCreating(true);
    try {
      const data = await createUserKey(name.trim(), textLimit, imageLimit, textConcurrency, imageConcurrency, createTextUnlimited, createImageUnlimited);
      setItems(data.items);
      setRevealedKey(data.key);
      setName("");
      setCreateTextLimit("0");
      setCreateTextUnlimited(true);
      setCreateImageLimit("0");
      setCreateImageUnlimited(true);
      setCreateTextConcurrency("4");
      setCreateImageConcurrency("1");
      setIsDialogOpen(false);
      toast.success("用户密钥已创建");
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "创建用户密钥失败");
    } finally {
      setIsCreating(false);
    }
  };

  const setItemPending = (id: string, isPending: boolean) => {
    setPendingIds((current) => {
      const next = new Set(current);
      if (isPending) {
        next.add(id);
      } else {
        next.delete(id);
      }
      return next;
    });
  };

  const handleToggle = async (item: UserKey) => {
    setItemPending(item.id, true);
    try {
      const data = await updateUserKey(item.id, { enabled: !item.enabled });
      setItems(data.items);
      toast.success(item.enabled ? "用户密钥已禁用" : "用户密钥已启用");
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "更新用户密钥失败");
    } finally {
      setItemPending(item.id, false);
    }
  };

  const handleDelete = async () => {
    if (!deletingItem) {
      return;
    }
    const item = deletingItem;
    setItemPending(item.id, true);
    try {
      const data = await deleteUserKey(item.id);
      setItems(data.items);
      setDeletingItem(null);
      toast.success("用户密钥已删除");
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "删除用户密钥失败");
    } finally {
      setItemPending(item.id, false);
    }
  };

  const openEditDialog = (item: UserKey) => {
    setEditingItem(item);
    setEditName(item.name);
    setEditKey("");
    setEditTextLimit(String(item.text_limit ?? 0));
    setEditTextUnlimited(Boolean(item.text_unlimited));
    setEditImageLimit(String(item.image_limit ?? 0));
    setEditImageUnlimited(Boolean(item.image_unlimited));
    setEditTextConcurrency(String(item.text_concurrency ?? 4));
    setEditImageConcurrency(String(item.image_concurrency ?? 1));
    setEditWebLoginEnabled(item.web_login_enabled);
  };

  const handleEdit = async () => {
    if (!editingItem) {
      return;
    }
    const item = editingItem;
    const trimmedName = editName.trim();
    const trimmedKey = editKey.trim();
    const textLimit = editTextUnlimited ? item.text_limit : parseQuotaLimit(editTextLimit);
    const imageLimit = editImageUnlimited ? item.image_limit : parseQuotaLimit(editImageLimit);
    const textConcurrency = parseConcurrencyLimit(editTextConcurrency);
    const imageConcurrency = parseConcurrencyLimit(editImageConcurrency);
    if (textLimit === null || imageLimit === null) {
      toast.error("额度必须是大于等于 0 的整数");
      return;
    }
    if (textConcurrency === null || imageConcurrency === null) {
      toast.error("并发必须是大于等于 1 的整数");
      return;
    }
    const limitsChanged = textLimit !== item.text_limit || imageLimit !== item.image_limit || editTextUnlimited !== item.text_unlimited || editImageUnlimited !== item.image_unlimited;
    const concurrencyChanged =
      textConcurrency !== item.text_concurrency || imageConcurrency !== item.image_concurrency;
    const webLoginChanged = editWebLoginEnabled !== item.web_login_enabled;
    if (trimmedName === item.name && !trimmedKey && !limitsChanged && !concurrencyChanged && !webLoginChanged) {
      setEditingItem(null);
      return;
    }
    setItemPending(item.id, true);
    try {
      const data = await updateUserKey(item.id, {
        ...(trimmedName !== item.name ? { name: trimmedName } : {}),
        ...(trimmedKey ? { key: trimmedKey } : {}),
        ...(limitsChanged ? { text_limit: textLimit, image_limit: imageLimit, text_unlimited: editTextUnlimited, image_unlimited: editImageUnlimited } : {}),
        ...(concurrencyChanged
          ? { text_concurrency: textConcurrency, image_concurrency: imageConcurrency }
          : {}),
        ...(webLoginChanged ? { web_login_enabled: editWebLoginEnabled } : {}),
      });
      setItems(data.items);
      setEditingItem(null);
      setEditKey("");
      toast.success(
        webLoginChanged
          ? "用户密钥与 Web 登录权限已更新"
          : limitsChanged || concurrencyChanged
            ? "用户密钥额度与并发已更新"
            : trimmedKey
              ? "用户密钥已更新"
              : "用户名称已更新",
      );
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "更新用户密钥失败");
    } finally {
      setItemPending(item.id, false);
    }
  };

  const handleCopy = async (value: string) => {
    try {
      await navigator.clipboard.writeText(value);
      toast.success("已复制到剪贴板");
    } catch {
      toast.error("复制失败，请手动复制");
    }
  };

  return (
    <>
      <Card className="rounded-sm border-white/80 bg-white/90 shadow-sm">
        <CardContent className="space-y-6 p-6">
          <div className="flex items-start justify-between gap-4">
            <div className="flex items-center gap-3">
              <div className="flex size-10 items-center justify-center rounded-sm bg-stone-100">
                <KeyRound className="size-5 text-stone-600" />
              </div>
              <div>
                <h2 className="text-lg font-semibold tracking-tight">用户密钥管理</h2>
                <p className="text-sm text-stone-500">为普通用户创建专用密钥；可分别控制 API 使用状态和 Web 登录权限。</p>
              </div>
            </div>
            <Button className="h-9 rounded-sm bg-stone-950 px-4 text-white hover:bg-stone-800" onClick={() => setIsDialogOpen(true)}>
              <Plus className="size-4" />
              创建用户密钥
            </Button>
          </div>

          {revealedKey ? (
            <div className="rounded-sm border border-emerald-200 bg-emerald-50 px-4 py-4 text-sm text-emerald-900">
              <div className="font-medium">新密钥仅展示一次，请立即保存：</div>
              <div className="mt-3 flex flex-col gap-3 rounded-lg border border-emerald-200 bg-white/80 p-3 md:flex-row md:items-center md:justify-between">
                <code className="break-all font-mono text-[13px]">{revealedKey}</code>
                <Button
                  type="button"
                  variant="outline"
                  className="h-9 rounded-sm border-emerald-200 bg-white px-4 text-emerald-700"
                  onClick={() => void handleCopy(revealedKey)}
                >
                  <Copy className="size-4" />
                  复制
                </Button>
              </div>
            </div>
          ) : null}

          {isLoading ? (
            <div className="flex items-center justify-center py-10">
              <LoaderCircle className="size-5 animate-spin text-stone-400" />
            </div>
          ) : items.length === 0 ? (
            <div className="rounded-sm bg-stone-50 px-6 py-10 text-center text-sm text-stone-500">
              暂无普通用户密钥。点击右上角按钮后即可创建并分发给其他人。
            </div>
          ) : (
            <div className="space-y-3">
              {items.map((item) => {
                const isPending = pendingIds.has(item.id);
                return (
                  <div key={item.id} className="flex flex-col gap-3 rounded-sm border border-stone-200 bg-white px-4 py-4 md:flex-row md:items-center md:justify-between">
                    <div className="min-w-0 space-y-2">
                      <div className="flex flex-wrap items-center gap-2">
                        <div className="truncate text-sm font-medium text-stone-800">{item.name}</div>
                        <Badge variant={item.enabled ? "success" : "secondary"} className="rounded-md">
                          {item.enabled ? "已启用" : "已禁用"}
                        </Badge>
                        <Badge variant={item.web_login_enabled ? "info" : "secondary"} className="rounded-md">
                          {item.web_login_enabled ? "允许 Web 登录" : "禁止 Web 登录"}
                        </Badge>
                      </div>
                      <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-stone-500">
                        <span>创建时间 {formatDateTime(item.created_at)}</span>
                        <span>最近使用 {formatDateTime(item.last_used_at)}</span>
                      </div>
                      <div className="grid max-w-xl grid-cols-1 gap-x-6 gap-y-2 border-t border-stone-100 pt-2 sm:grid-cols-2">
                        <QuotaSummary
                          label="文本额度"
                          used={item.text_used}
                          reserved={item.text_reserved}
                          remaining={item.text_remaining}
                          unlimited={item.text_unlimited}
                          limit={item.text_limit}
                        />
                        <QuotaSummary
                          label="图片额度（生成+编辑）"
                          used={item.image_used}
                          reserved={item.image_reserved}
                          remaining={item.image_remaining}
                          unlimited={item.image_unlimited}
                          limit={item.image_limit}
                        />
                        <ConcurrencySummary label="文本并发" limit={item.text_concurrency} />
                        <ConcurrencySummary label="图片并发" limit={item.image_concurrency} />
                      </div>
                    </div>

                    <div className="flex items-center gap-2">
                      <Button
                        type="button"
                        variant="outline"
                        className="h-9 rounded-sm border-stone-200 bg-white px-4 text-stone-700"
                        onClick={() => openEditDialog(item)}
                        disabled={isPending}
                      >
                        {isPending ? <LoaderCircle className="size-4 animate-spin" /> : <Pencil className="size-4" />}
                        编辑
                      </Button>
                      <Button
                        type="button"
                        variant="outline"
                        className="h-9 rounded-sm border-stone-200 bg-white px-4 text-stone-700"
                        onClick={() => void handleToggle(item)}
                        disabled={isPending}
                      >
                        {isPending ? (
                          <LoaderCircle className="size-4 animate-spin" />
                        ) : item.enabled ? (
                          <Ban className="size-4" />
                        ) : (
                          <CheckCircle2 className="size-4" />
                        )}
                        {item.enabled ? "禁用" : "启用"}
                      </Button>
                      <Button
                        type="button"
                        variant="outline"
                        className="h-9 rounded-sm border-rose-200 bg-white px-4 text-rose-600 hover:bg-rose-50 hover:text-rose-700"
                        onClick={() => setDeletingItem(item)}
                        disabled={isPending}
                      >
                        {isPending ? <LoaderCircle className="size-4 animate-spin" /> : <Trash2 className="size-4" />}
                        删除
                      </Button>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </CardContent>
      </Card>

      <Dialog open={isDialogOpen} onOpenChange={setIsDialogOpen}>
        <DialogContent className="rounded-sm p-6">
          <DialogHeader className="gap-2">
            <DialogTitle>创建用户密钥</DialogTitle>
            <DialogDescription className="text-sm leading-6">
              新账号默认不限文本和图片业务额度；仍受上游可用额度及并发限制。可取消“无限制”后设置总额度。
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-2">
            <label className="text-sm font-medium text-stone-700">名称（可选）</label>
            <Input
              value={name}
              onChange={(event) => setName(event.target.value)}
              placeholder="例如：设计同学 A、运营临时账号"
              className="h-11 rounded-sm border-stone-200 bg-white"
            />
          </div>
          <div className="grid gap-4 sm:grid-cols-2">
            <div className="space-y-2">
              <label className="text-sm font-medium text-stone-700" htmlFor="create-text-limit">
                文本额度（总次数）
              </label>
              <label className="flex items-center gap-2 text-sm"><Checkbox checked={createTextUnlimited} onCheckedChange={(checked) => setCreateTextUnlimited(checked === true)} />无限制</label>
              <Input
                id="create-text-limit"
                type="number"
                min={0}
                step={1}
                inputMode="numeric"
                disabled={createTextUnlimited}
                value={createTextLimit}
                onChange={(event) => setCreateTextLimit(event.target.value)}
                className="h-11 rounded-sm border-stone-200 bg-white"
              />
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium text-stone-700" htmlFor="create-image-limit">
                图片额度（总张数）
              </label>
              <label className="flex items-center gap-2 text-sm"><Checkbox checked={createImageUnlimited} onCheckedChange={(checked) => setCreateImageUnlimited(checked === true)} />无限制</label>
              <Input
                id="create-image-limit"
                type="number"
                min={0}
                step={1}
                inputMode="numeric"
                disabled={createImageUnlimited}
                value={createImageLimit}
                onChange={(event) => setCreateImageLimit(event.target.value)}
                className="h-11 rounded-sm border-stone-200 bg-white"
              />
            </div>
          </div>
          <div className="grid gap-4 sm:grid-cols-2">
            <div className="space-y-2">
              <label className="text-sm font-medium text-stone-700" htmlFor="create-text-concurrency">
                文本并发
              </label>
              <Input
                id="create-text-concurrency"
                type="number"
                min={1}
                step={1}
                inputMode="numeric"
                value={createTextConcurrency}
                onChange={(event) => setCreateTextConcurrency(event.target.value)}
                className="h-11 rounded-sm border-stone-200 bg-white"
              />
              <p className="text-xs text-stone-500">该密钥同时处理的文本请求数量。</p>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium text-stone-700" htmlFor="create-image-concurrency">
                图片并发
              </label>
              <Input
                id="create-image-concurrency"
                type="number"
                min={1}
                step={1}
                inputMode="numeric"
                value={createImageConcurrency}
                onChange={(event) => setCreateImageConcurrency(event.target.value)}
                className="h-11 rounded-sm border-stone-200 bg-white"
              />
              <p className="text-xs text-stone-500">该密钥同时处理的图片请求数量。</p>
            </div>
          </div>
          <DialogFooter>
            <Button
              type="button"
              variant="secondary"
              className="h-10 rounded-sm bg-stone-100 px-5 text-stone-700 hover:bg-stone-200"
              onClick={() => setIsDialogOpen(false)}
              disabled={isCreating}
            >
              取消
            </Button>
            <Button
              type="button"
              className="h-10 rounded-sm bg-stone-950 px-5 text-white hover:bg-stone-800"
              onClick={() => void handleCreate()}
              disabled={isCreating}
            >
              {isCreating ? <LoaderCircle className="size-4 animate-spin" /> : <Plus className="size-4" />}
              创建
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={Boolean(deletingItem)} onOpenChange={(open) => (!open ? setDeletingItem(null) : null)}>
        <DialogContent className="rounded-sm p-6">
          <DialogHeader className="gap-2">
            <DialogTitle>删除用户密钥</DialogTitle>
            <DialogDescription className="text-sm leading-6">
              确认删除用户密钥「{deletingItem?.name}」吗？删除后该密钥将无法继续调用接口。
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              type="button"
              variant="secondary"
              className="h-10 rounded-sm bg-stone-100 px-5 text-stone-700 hover:bg-stone-200"
              onClick={() => setDeletingItem(null)}
              disabled={deletingItem ? pendingIds.has(deletingItem.id) : false}
            >
              取消
            </Button>
            <Button
              type="button"
              className="h-10 rounded-sm bg-rose-600 px-5 text-white hover:bg-rose-700"
              onClick={() => void handleDelete()}
              disabled={deletingItem ? pendingIds.has(deletingItem.id) : false}
            >
              {deletingItem && pendingIds.has(deletingItem.id) ? <LoaderCircle className="size-4 animate-spin" /> : <Trash2 className="size-4" />}
              删除
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog
        open={Boolean(editingItem)}
        onOpenChange={(open) => {
          if (!open) {
            setEditingItem(null);
            setEditKey("");
          }
        }}
      >
        <DialogContent className="rounded-sm p-6">
          <DialogHeader className="gap-2">
            <DialogTitle>编辑用户密钥</DialogTitle>
            <DialogDescription className="text-sm leading-6">
              可以修改备注名称；如需更换专用密钥，直接填写新的原始密钥即可。留空则保持当前密钥不变。
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="space-y-2">
              <label className="text-sm font-medium text-stone-700">名称</label>
              <Input
                value={editName}
                onChange={(event) => setEditName(event.target.value)}
                placeholder="例如：设计同学 A、运营临时账号"
                className="h-11 rounded-sm border-stone-200 bg-white"
              />
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium text-stone-700">新的专用密钥（可选）</label>
              <Input
                value={editKey}
                onChange={(event) => setEditKey(event.target.value)}
                placeholder="例如：sk-your-custom-user-key"
                className="h-11 rounded-sm border-stone-200 bg-white font-mono"
              />
              <p className="text-xs leading-5 text-stone-500">
                保存后旧密钥会立即失效，新密钥生效。系统仍只保存哈希，不会回显当前密钥。
              </p>
            </div>
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="space-y-2">
                <label className="text-sm font-medium text-stone-700" htmlFor="edit-text-limit">
                  文本额度（总次数）
                </label>
                <label className="flex items-center gap-2 text-sm"><Checkbox checked={editTextUnlimited} onCheckedChange={(checked) => setEditTextUnlimited(checked === true)} />无限制</label>
              <Input
                  id="edit-text-limit"
                  type="number"
                  min={0}
                  step={1}
                  inputMode="numeric"
                  disabled={editTextUnlimited}
                value={editTextLimit}
                  onChange={(event) => setEditTextLimit(event.target.value)}
                  className="h-11 rounded-sm border-stone-200 bg-white"
                />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium text-stone-700" htmlFor="edit-image-limit">
                  图片额度（总张数）
                </label>
                <label className="flex items-center gap-2 text-sm"><Checkbox checked={editImageUnlimited} onCheckedChange={(checked) => setEditImageUnlimited(checked === true)} />无限制</label>
              <Input
                  id="edit-image-limit"
                  type="number"
                  min={0}
                  step={1}
                  inputMode="numeric"
                  disabled={editImageUnlimited}
                value={editImageLimit}
                  onChange={(event) => setEditImageLimit(event.target.value)}
                  className="h-11 rounded-sm border-stone-200 bg-white"
                />
              </div>
            </div>
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="space-y-2">
                <label className="text-sm font-medium text-stone-700" htmlFor="edit-text-concurrency">
                  文本并发
                </label>
                <Input
                  id="edit-text-concurrency"
                  type="number"
                  min={1}
                  step={1}
                  inputMode="numeric"
                  value={editTextConcurrency}
                  onChange={(event) => setEditTextConcurrency(event.target.value)}
                  className="h-11 rounded-sm border-stone-200 bg-white"
                />
                <p className="text-xs text-stone-500">保存后对该密钥的新文本请求立即生效。</p>
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium text-stone-700" htmlFor="edit-image-concurrency">
                  图片并发
                </label>
                <Input
                  id="edit-image-concurrency"
                  type="number"
                  min={1}
                  step={1}
                  inputMode="numeric"
                  value={editImageConcurrency}
                  onChange={(event) => setEditImageConcurrency(event.target.value)}
                  className="h-11 rounded-sm border-stone-200 bg-white"
                />
                <p className="text-xs text-stone-500">保存后对该密钥的新图片请求立即生效。</p>
              </div>
            </div>
            <div className="flex items-start gap-3 rounded-sm border border-stone-200 bg-stone-50 px-4 py-3">
              <Checkbox
                id="edit-web-login-enabled"
                checked={editWebLoginEnabled}
                onCheckedChange={(checked) => setEditWebLoginEnabled(checked === true)}
                disabled={editingItem ? pendingIds.has(editingItem.id) : false}
                className="mt-0.5"
              />
              <div className="space-y-1">
                <label htmlFor="edit-web-login-enabled" className="text-sm font-medium text-stone-700">
                  允许 Web 登录
                </label>
                <p className="text-xs leading-5 text-stone-500">
                  密钥保持启用时，关闭此项仍可调用 API，但不能用于登录 Web。
                </p>
              </div>
            </div>
          </div>
          <DialogFooter>
            <Button
              type="button"
              variant="secondary"
              className="h-10 rounded-sm bg-stone-100 px-5 text-stone-700 hover:bg-stone-200"
              onClick={() => {
                setEditingItem(null);
                setEditKey("");
              }}
              disabled={editingItem ? pendingIds.has(editingItem.id) : false}
            >
              取消
            </Button>
            <Button
              type="button"
              className="h-10 rounded-sm bg-stone-950 px-5 text-white hover:bg-stone-800"
              onClick={() => void handleEdit()}
              disabled={editingItem ? pendingIds.has(editingItem.id) : false}
            >
              {editingItem && pendingIds.has(editingItem.id) ? <LoaderCircle className="size-4 animate-spin" /> : <Pencil className="size-4" />}
              保存
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
