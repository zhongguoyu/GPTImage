"use client";
import { usePathname } from "next/navigation";
import { TopNav } from "@/components/top-nav";

export function AppShell({children}: {children: React.ReactNode}) {
  const pathname=usePathname().replace(/\/+$/, "") || "/";
  const login=pathname==="/login" || pathname==="/";
  return <div className={login ? "console-shell console-auth-shell" : "console-shell"}>
    <a href="#main-content" className="console-skip-link">跳转到主要内容</a>
    <TopNav />
    <main id="main-content" className={"console-main"+(pathname==="/image" ? " console-image-main" : "")} tabIndex={-1}>{children}</main>
  </div>;
}
