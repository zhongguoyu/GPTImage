from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class StorageBackend(ABC):
    """抽象存储后端基类"""

    @abstractmethod
    def load_accounts(self) -> list[dict[str, Any]]:
        """加载所有账号数据"""
        pass

    @abstractmethod
    def save_accounts(self, accounts: list[dict[str, Any]]) -> None:
        """保存所有账号数据"""
        pass

    def save_account(self, account: dict[str, Any]) -> None:
        """保存单个账号；旧后端使用快照兼容实现。"""
        if not isinstance(account, dict):
            return
        access_token = str(account.get("access_token") or account.get("accessToken") or "").strip()
        if not access_token:
            return
        accounts = self.load_accounts()
        replaced = False
        for index, item in enumerate(accounts):
            item_token = str(item.get("access_token") or item.get("accessToken") or "").strip()
            if item_token == access_token:
                accounts[index] = dict(account)
                replaced = True
                break
        if not replaced:
            accounts.append(dict(account))
        self.save_accounts(accounts)

    def delete_account(self, access_token: str) -> None:
        target = str(access_token or "").strip()
        if not target:
            return
        self.save_accounts([
            item for item in self.load_accounts()
            if str(item.get("access_token") or item.get("accessToken") or "").strip() != target
        ])

    @abstractmethod
    def load_auth_keys(self) -> list[dict[str, Any]]:
        """加载所有鉴权密钥数据"""
        pass

    @abstractmethod
    def save_auth_keys(self, auth_keys: list[dict[str, Any]]) -> None:
        """保存所有鉴权密钥数据"""
        pass

    def save_auth_key(self, auth_key: dict[str, Any]) -> None:
        """保存单个鉴权密钥；旧后端使用快照兼容实现。"""
        if not isinstance(auth_key, dict):
            return
        key_id = str(auth_key.get("id") or "").strip()
        if not key_id:
            return
        items = self.load_auth_keys()
        replaced = False
        for index, item in enumerate(items):
            if str(item.get("id") or "").strip() == key_id:
                items[index] = dict(auth_key)
                replaced = True
                break
        if not replaced:
            items.append(dict(auth_key))
        self.save_auth_keys(items)

    def delete_auth_key(self, key_id: str) -> None:
        target = str(key_id or "").strip()
        if not target:
            return
        self.save_auth_keys([
            item for item in self.load_auth_keys()
            if str(item.get("id") or "").strip() != target
        ])

    @abstractmethod
    def health_check(self) -> dict[str, Any]:
        """健康检查，返回存储后端状态"""
        pass

    @abstractmethod
    def get_backend_info(self) -> dict[str, Any]:
        """获取存储后端信息"""
        pass
