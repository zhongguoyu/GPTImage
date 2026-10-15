"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { Menu, ImagePlus, Users, Images, ScrollText, Terminal, Settings, Layers, LogOut, Aperture, ChevronRight } from "lucide-react";
import { usePathname, useRouter } from "next/navigation";

import { HeaderActions } from "@/components/header-actions";
import { Button } from "@/components/ui/button";
import { Dialog, DialogClose, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Sheet, SheetClose, SheetContent, SheetFooter, SheetHeader, SheetTitle, SheetTrigger } from "@/components/ui/sheet";
import webConfig from "@/constants/common-env";
import { fetchThirdPartyApps, type ThirdPartyAppsSettings } from "@/lib/api";
import { getValidatedAuthSession } from "@/lib/auth-session";
import { cn } from "@/lib/utils";
import { clearStoredAuthSession, type StoredAuthSession } from "@/store/auth";

const adminNavItems = [
  { href: "/image", label: "生图" },
  { href: "/accounts", label: "号池管理" },
  { href: "/image-manager", label: "图片管理" },
  { href: "/logs", label: "日志管理" },
  { href: "/debug", label: "调试" },
  { href: "/settings", label: "设置" },
];

const navIcons = { "/image": ImagePlus, "/accounts": Users, "/image-manager": Images, "/logs": ScrollText, "/debug": Terminal, "/settings": Settings };

const userNavItems = [{ href: "/image", label: "画图" }];

function buildThirdPartyHref(appUrl: string, baseUrl: string, apiKey: string) {
  const url = appUrl.trim();
  try {
    const target = new URL(url);
    target.searchParams.set("apiKey", apiKey);
    target.searchParams.set("baseUrl", baseUrl);
    return target.toString();
  } catch {
    return `${url}${url.includes("?") ? "&" : "?"}apiKey=${encodeURIComponent(apiKey)}&baseUrl=${encodeURIComponent(baseUrl)}`;
  }
}

export function TopNav() {
  const pathname = usePathname().replace(/\/+$/, "") || "/";
  const router = useRouter();
  const [session, setSession] = useState<StoredAuthSession | null | undefined>(undefined);
  const [thirdPartyApps, setThirdPartyApps] = useState<ThirdPartyAppsSettings | null>(null);
  const [isCanvasDialogOpen, setIsCanvasDialogOpen] = useState(false);

  useEffect(() => {
    let active = true;

    const load = async () => {
      if (pathname === "/login") {
        if (!active) {
          return;
        }
        setSession(null);
        return;
      }

      const storedSession = await getValidatedAuthSession();
      if (!active) {
        return;
      }
      setSession(storedSession);
    };

    void load();
    return () => {
      active = false;
    };
  }, [pathname]);

  useEffect(() => {
    if (!session) {
      setThirdPartyApps(null);
      return;
    }
    let active = true;
    const load = async () => {
      try {
        const data = await fetchThirdPartyApps();
        if (active) {
          setThirdPartyApps(data.third_party_apps);
        }
      } catch {
        if (active) {
          setThirdPartyApps(null);
        }
      }
    };
    const reload = () => void load();

    void load();
    window.addEventListener("third-party-apps-updated", reload);
    return () => {
      active = false;
      window.removeEventListener("third-party-apps-updated", reload);
    };
  }, [session]);

  const handleLogout = async () => {
    await clearStoredAuthSession();
    router.replace("/login");
  };

  if (pathname === "/login" || session === undefined || !session) {
    return null;
  }

  const navItems = session.role === "admin" ? adminNavItems : userNavItems;
  const roleLabel = session.role === "admin" ? "管理员" : "普通用户";
  const displayName = session.name.trim() || roleLabel;
  const baseUrl = webConfig.apiUrl.replace(/\/$/, "") || window.location.origin;
  const canvas = thirdPartyApps?.infinite_canvas;
  const canvasHref = canvas?.enabled && canvas.url.trim() ? buildThirdPartyHref(canvas.url, baseUrl, session.key) : "";
  const canvasDisplayHref = canvasHref ? decodeURIComponent(canvasHref) : "";

  const handleCanvasOpen = () => {
    if (!canvasHref) {
      return;
    }
    setIsCanvasDialogOpen(true);
  };

  const confirmCanvasOpen = () => {
    if (canvasHref) {
      window.open(canvasHref, "_blank", "noopener,noreferrer");
    }
    setIsCanvasDialogOpen(false);
  };

  return (
    <>
      <aside className="console-sidebar" aria-label="主导航">
        <Link href="/image" className="console-brand">
          <span className="console-mark"><Aperture size={23} /></span>
          <span><strong>GPTImage</strong><small>IMAGE WORKSPACE</small></span>
        </Link>
        <div className="console-nav-caption">工作空间 <span>/ WORKSPACE</span></div>
        <nav className="console-nav">
          {navItems.map((item) => {
            const Icon = navIcons[item.href as keyof typeof navIcons];
            return <Link key={item.href} href={item.href} aria-current={pathname === item.href ? "page" : undefined} className={cn("console-nav-link", pathname === item.href && "is-active")}>
              <Icon size={17} /><span>{item.label}</span><ChevronRight className="console-nav-chevron" size={13} />
            </Link>;
          })}
          {canvasHref && <button type="button" className="console-nav-link" onClick={handleCanvasOpen}><Layers size={17} /><span>无限画布</span></button>}
        </nav>
        <div className="console-sidebar-bottom">
          <div className="console-stack-note"><span className="console-square" /> GPTImage<small>ChatGPT2API × Sub2API</small></div>
          <div className="console-user"><span className="console-avatar">{displayName.slice(0, 1)}</span><span className="min-w-0"><strong className="block truncate">{displayName}</strong><small>{roleLabel}</small></span><button type="button" onClick={() => void handleLogout()} aria-label="退出登录" title="退出登录"><LogOut size={16} /></button></div>
        </div>
      </aside>
      <header className="console-topbar">
        <div className="flex min-w-0 items-center gap-3">
          <Sheet>
            <SheetTrigger className="console-mobile-trigger" aria-label="打开导航"><Menu size={20} /></SheetTrigger>
            <SheetContent side="left">
              <SheetHeader><SheetTitle>GPTImage</SheetTitle><span className="text-xs text-muted-foreground">{roleLabel} · {displayName}</span></SheetHeader>
              <nav className="console-nav mt-6" aria-label="移动端主导航">
                {navItems.map((item) => {
                  const Icon=navIcons[item.href as keyof typeof navIcons];
                  return <SheetClose asChild key={item.href}><Link href={item.href} aria-current={pathname === item.href ? "page" : undefined} className={cn("console-nav-link", pathname === item.href && "is-active")}><Icon size={17} />{item.label}</Link></SheetClose>;
                })}
                {canvasHref && <SheetClose asChild><button className="console-nav-link" onClick={handleCanvasOpen}><Layers size={17} />无限画布</button></SheetClose>}
              </nav>
              <SheetFooter><Button variant="outline" onClick={() => void handleLogout()}><LogOut size={16} />退出</Button></SheetFooter>
            </SheetContent>
          </Sheet>
          <span className="console-breadcrumb">工作空间 <span>/</span> <strong>{navItems.find(item=>item.href===pathname)?.label || "GPTImage"}</strong></span>
        </div>
        <HeaderActions />
      </header>
      <Dialog open={isCanvasDialogOpen} onOpenChange={setIsCanvasDialogOpen}>
        <DialogContent showCloseButton={false} className="rounded-sm p-6">
          <DialogHeader className="gap-2">
            <DialogTitle>跳转到三方应用</DialogTitle>
            <DialogDescription className="text-sm leading-6">
              该入口仅供个人测试使用，建议自行本机部署后再长期使用。跳转地址会默认带上本项目地址和当前密钥，用于自动填充连接信息；如果不放心，可以取消后手动前往应用并自行输入。
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-2">
            <div className="text-xs font-medium text-stone-500">完整跳转地址</div>
            <div className="max-h-28 overflow-auto break-all rounded-sm border border-stone-200 bg-stone-50 px-3 py-2 font-mono text-xs leading-5 text-stone-700">
              {canvasDisplayHref}
            </div>
          </div>
          <DialogFooter className="pt-2">
            <DialogClose asChild>
              <Button type="button" variant="outline" className="rounded-sm border-stone-200 bg-white text-stone-700">
                取消
              </Button>
            </DialogClose>
            <Button type="button" className="rounded-sm bg-stone-950 text-white hover:bg-stone-800" onClick={confirmCanvasOpen}>
              继续跳转
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
