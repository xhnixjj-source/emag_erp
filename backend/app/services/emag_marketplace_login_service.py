import base64
import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from playwright.sync_api import BrowserContext, Page
from sqlalchemy.orm import Session

from app.utils.playwright_manager import get_playwright_pool
from app.utils.bitbrowser_manager import bitbrowser_manager
from app.config import config, get_project_root
from app.models.emag_sync import EmagInboundShipment, EmagInboundShipmentDetail
from app.models.emag_sync import EmagInboundShipment, EmagInboundShipmentDetail

logger = logging.getLogger(__name__)


@dataclass
class LoginPageInfo:
    url: str
    title: str
    seller_hint: Optional[str] = None


class EmagMarketplaceLoginService:
    """
    eMAG Marketplace 后台网页登录服务（单例）。

    目标：
    - 用 Playwright 打开登录页，填写账号密码并提交
    - 判断是否登录成功 / 是否需要验证码
    - 登录成功后抓取一段页面信息用于前端确认
    - 保持浏览器会话（context + page）存活，供后续抓取复用
    """

    _instance: Optional["EmagMarketplaceLoginService"] = None
    _instance_lock = threading.Lock()

    MARKETPLACE_HOME = "https://marketplace.emag.ro/"
    DASHBOARD_URL = "https://marketplace.emag.ro/dashboard"
    AUTH_STORAGE_FILE = "emag_marketplace_auth.json"  # 默认登录状态文件（无 shop_id 时兼容）

    def __new__(cls) -> "EmagMarketplaceLoginService":
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if getattr(self, "_initialized", False):
            return
        self._lock = threading.RLock()
        self._pool = get_playwright_pool()
        # 执行锁：确保同一时间只有一个 login() 在执行
        self._login_execution_lock = threading.Lock()

        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None
        self._owner_thread_id: Optional[int] = None
        self._window_id: Optional[str] = None
        self._cdp_ws: Optional[str] = None
        # 用于手动登录模式的独立 browser 实例（不使用 pool）
        self._playwright_instance = None
        self._browser_instance = None
        # 用于手动登录模式的独立 browser 实例（不使用 pool）
        self._playwright_instance = None
        self._browser_instance = None

        self._status: str = "not_logged_in"  # not_logged_in|logging_in|auto_filling|waiting_manual_login|logged_in|error
        self._last_error: Optional[str] = None
        self._last_captcha_png_b64: Optional[str] = None
        self._last_page_info: Optional[LoginPageInfo] = None
        self._current_shop_id: Optional[int] = None  # 当前登录的店铺 ID
        
        # 入仓运单同步结果
        self._last_sync_result: Optional[Dict[str, Any]] = None
        self._sync_status: str = "idle"  # idle|syncing|completed|error
        
        # 登录状态文件路径
        project_root = get_project_root()
        self._auth_storage_path = project_root / self.AUTH_STORAGE_FILE

        self._initialized = True

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def _get_auth_storage_path(self, shop_id: Optional[int] = None) -> Path:
        """获取登录状态文件路径。每个店铺独立一份 auth 文件。"""
        project_root = get_project_root()
        if shop_id:
            return project_root / f"emag_marketplace_auth_shop_{shop_id}.json"
        return project_root / self.AUTH_STORAGE_FILE

    def set_current_shop_id(self, shop_id: Optional[int]) -> None:
        """设置当前登录关联的店铺 ID，并切换 auth 存储路径"""
        with self._lock:
            self._current_shop_id = shop_id
            self._auth_storage_path = self._get_auth_storage_path(shop_id)

    def get_current_shop_id(self) -> Optional[int]:
        """获取当前登录关联的店铺 ID"""
        with self._lock:
            return self._current_shop_id

    def get_login_status(self) -> Dict[str, Any]:
        with self._lock:
            info = self._last_page_info.__dict__ if self._last_page_info else None
            return {
                "status": self._status,
                "error": self._last_error,
                "captcha_screenshot_b64": self._last_captcha_png_b64,
                "page_info": info,
                "shop_id": self._current_shop_id,
            }
    
    def get_sync_status(self) -> Dict[str, Any]:
        """获取入仓运单同步状态和结果"""
        with self._lock:
            return {
                "sync_status": self._sync_status,
                "last_sync_result": self._last_sync_result,
            }

    def logout(self) -> None:
        with self._lock:
            self._status = "not_logged_in"
            self._last_error = None
            self._last_captcha_png_b64 = None
            self._last_page_info = None
            self._close_session_locked()

        # 删除保存的登录状态，确保下次登录时不会自动登录旧账号
        if self._auth_storage_path.exists():
            try:
                self._auth_storage_path.unlink()
                logger.info(f"已删除保存的登录状态: {self._auth_storage_path}")
            except Exception as e:
                logger.warning(f"删除登录状态文件失败: {e}")

    def login(self, username: str = None, password: str = None) -> Dict[str, Any]:
        """
        执行登录流程：
        1. 先以 headless 模式启动浏览器，尝试自动填充用户名密码
        2. 如遇到 SMS 验证码 → 返回状态让前端输入
        3. 如遇到 CAPTCHA 或自动填充失败 → 切换到 headful 模式（弹窗 fallback）
        
        注意：此方法必须在独立线程中调用（不能在 asyncio 事件循环中），
        因为 sync_playwright() 不兼容 asyncio 环境。
        路由层使用 threading.Thread 来调用此方法。
        """
        if not self._login_execution_lock.acquire(blocking=False):
            logger.info("登录正在进行中，返回当前状态")
            return self.get_login_status()
        
        try:
            with self._lock:
                self._status = "logging_in"
                self._last_error = None
                self._last_captcha_png_b64 = None
                self._last_page_info = None

            from playwright.sync_api import sync_playwright
            
            logger.info("开始登录流程（headless 自动填充模式）")
            
            # ── 阶段 1: headless 自动填充 ──
            playwright_instance = sync_playwright().start()
            browser = playwright_instance.chromium.launch(headless=True)
            
            if self._auth_storage_path.exists():
                try:
                    context = browser.new_context(storage_state=str(self._auth_storage_path))
                    logger.info("已加载保存的登录状态")
                except Exception:
                    context = browser.new_context()
            else:
                context = browser.new_context()
            
            page = context.new_page()
            need_fallback = False  # 是否需要弹窗 fallback
            
            try:
                page.goto(self.MARKETPLACE_HOME, wait_until="domcontentloaded", timeout=20000)
                time.sleep(1)
                
                current_url = page.url or ""
                logger.info(f"导航完成，当前 URL: {current_url}")
                
                # #region agent log
                import json as _dbg_j6; _dbg_lp6 = r"d:\emag_erp\.cursor\debug.log"
                def _dbg_w6(loc, msg, data, hyp):
                    try:
                        import time as _t
                        with open(_dbg_lp6, "a", encoding="utf-8") as _f:
                            _f.write(_dbg_j6.dumps({"timestamp": int(_t.time()*1000), "location": loc, "message": msg, "data": data, "hypothesisId": hyp}, ensure_ascii=False) + "\n")
                    except Exception:
                        pass
                _is_li = self._is_logged_in(page)
                _has_dash = "dashboard" in current_url
                _dbg_w6("login_svc:login:phase1_check", "login state check at phase 1", {"url": current_url, "_is_logged_in": _is_li, "dashboard_in_url": _has_dash, "username_provided": bool(username), "password_provided": bool(password)}, "H5,H7")
                # #endregion

                if _is_li or _has_dash:
                    # storage_state 仍然有效，直接登录成功
                    logger.info("storage_state 有效，已自动登录")
                elif username and password:
                    # 需要登录，尝试自动填充
                    with self._lock:
                        self._status = "auto_filling"
                    
                    logger.info("开始自动填充登录表单（多步登录）...")
                    auto_fill_ok = self._fill_login_form_multistep(page, username, password)
                    
                    # #region agent log
                    _dbg_w6("login_svc:login:auto_fill_result", "auto fill result", {"auto_fill_ok": auto_fill_ok, "url_after": page.url}, "H7")
                    # #endregion

                    if not auto_fill_ok:
                        logger.warning("自动填充登录表单失败，将切换到弹窗 fallback")
                        need_fallback = True
                    else:
                        # 自动填充完成，等待页面响应
                        time.sleep(1)
                        self._wait_after_submit(page)
                        current_url = page.url or ""
                        logger.info(f"表单提交后 URL: {current_url}")
                        
                        # 检查结果
                        if self._is_logged_in(page) or "dashboard" in current_url:
                            logger.info("自动填充后登录成功")
                        elif self._is_captcha_present(page):
                            logger.info("检测到 CAPTCHA，切换到弹窗 fallback")
                            need_fallback = True
                        elif self._is_sms_verification_required(page):
                            logger.info("检测到需要 SMS 验证码")
                            shot = self._screenshot_png_b64(page)
                            with self._lock:
                                self._status = "sms_verification_required"
                                self._last_captcha_png_b64 = shot
                                self._last_error = None
                                self._browser_instance = browser
                                self._playwright_instance = playwright_instance
                                self._context = context
                                self._page = page
                                self._owner_thread_id = threading.current_thread().ident
                            # 保持浏览器存活，等待用户输入验证码
                            return self.get_login_status()
                        else:
                            # 可能用户名密码错误或页面未跳转
                            logger.warning(f"自动填充后状态不明，当前 URL: {current_url}")
                            # 尝试截图看看当前页面状态
                            try:
                                shot = self._screenshot_png_b64(page)
                                logger.info("已截取当前页面截图用于诊断")
                            except Exception:
                                shot = None
                            need_fallback = True
                else:
                    # 没有提供用户名密码且 storage_state 失效
                    logger.info("未提供用户名密码且 storage_state 失效")
                    need_fallback = True
                
                if need_fallback:
                    # ── 阶段 2: 关闭 headless，切换到 headful fallback ──
                    logger.info("关闭 headless 浏览器，切换到 headful 弹窗模式...")
                    try:
                        browser.close()
                    except Exception:
                        pass
                    try:
                        playwright_instance.stop()
                    except Exception:
                        pass
                    
                    # 重新启动 headful 浏览器
                    playwright_instance = sync_playwright().start()
                    browser = playwright_instance.chromium.launch(headless=False)
                    
                    if self._auth_storage_path.exists():
                        try:
                            context = browser.new_context(storage_state=str(self._auth_storage_path))
                        except Exception:
                            context = browser.new_context()
                    else:
                        context = browser.new_context()
                    
                    page = context.new_page()
                    page.goto(self.MARKETPLACE_HOME, wait_until="domcontentloaded", timeout=20000)
                    time.sleep(1)
                    
                    # 如果有用户名密码，先尝试预填（方便用户直接提交）
                    if username and password and "dashboard" not in (page.url or ""):
                        try:
                            self._prefill_login_form(page, username, password)
                            logger.info("已在弹窗中预填用户名密码")
                        except Exception as e:
                            logger.warning(f"预填表单失败: {e}")
                    
                    with self._lock:
                        self._status = "waiting_manual_login"
                    
                    logger.info("等待用户在弹窗中手动完成登录...")
                    page.wait_for_url("**/dashboard**", timeout=0)
                
                # 到这里说明已登录成功（自动或手动）
                context.storage_state(path=str(self._auth_storage_path))
                logger.info(f"登录状态已保存到: {self._auth_storage_path}")
                
                page_info = self._collect_page_info(page)
                with self._lock:
                    self._status = "logged_in"
                    self._last_page_info = page_info
                    self._last_error = None
                    self._browser_instance = browser
                    self._playwright_instance = playwright_instance
                    self._context = context
                    self._page = page
                    self._owner_thread_id = threading.current_thread().ident
                
                logger.info("登录完成")
                return self.get_login_status()
                
            except Exception as e:
                logger.exception(f"登录过程中出错: {e}")
                try:
                    browser.close()
                except Exception:
                    pass
                try:
                    playwright_instance.stop()
                except Exception:
                    pass
                with self._lock:
                    self._status = "error"
                    self._last_error = str(e)
                return self.get_login_status()

        except Exception as e:
            logger.exception("eMAG marketplace login failed")
            with self._lock:
                self._status = "error"
                self._last_error = str(e)
            return self.get_login_status()
        finally:
            # 释放执行锁
            self._login_execution_lock.release()

    def captcha_done(self) -> Dict[str, Any]:
        """
        用户在弹出的浏览器窗口里手动完成验证码后调用，检查是否已登录成功。
        """
        with self._lock:
            if not self._page:
                self._status = "not_logged_in"
                self._last_error = "没有可用的浏览器会话"
                return self.get_login_status()

        try:
            page = self._page
            assert page is not None
            page.bring_to_front()

            # 给用户操作后留点时间，避免立刻检查失败
            time.sleep(0.8)
            self._wait_after_submit(page)

            # 检查是否需要手机验证码
            if self._is_sms_verification_required(page):
                logger.info("验证码完成后检测到需要手机验证码")
                shot = self._screenshot_png_b64(page)
                with self._lock:
                    self._status = "sms_verification_required"
                    self._last_captcha_png_b64 = shot
                    self._last_error = None
                return self.get_login_status()

            if self._is_logged_in(page):
                # 验证码确认后登录成功，等待一下让登录状态稳定，然后导航到 dashboard
                logger.info("验证码确认后登录成功，等待登录状态稳定...")
                time.sleep(2)  # 等待登录状态稳定
                
                # 再次检查登录状态
                if not self._is_logged_in(page):
                    logger.warning("等待后登录状态丢失，可能登录未完全成功")
                else:
                    # 导航到 dashboard（可能被重定向到登录页，需要再次导航）
                    max_retries = 3
                    for attempt in range(max_retries):
                        logger.info(f"尝试导航到 dashboard (第 {attempt + 1}/{max_retries} 次)")
                        try:
                            page.goto(self.DASHBOARD_URL, wait_until="domcontentloaded", timeout=15000)
                            time.sleep(2)  # 等待页面加载和可能的重定向
                            final_url = page.url
                            logger.info(f"导航完成，当前 URL: {final_url}")
                            
                            # 检查是否跳转到登录页
                            if "auth.emag" in final_url or "/login" in final_url:
                                logger.warning(f"导航后跳转到登录页: {final_url}")
                                if attempt < max_retries - 1:
                                    # 等待一下，然后再次尝试导航
                                    logger.info(f"等待 3 秒后再次尝试导航到 dashboard...")
                                    time.sleep(3)
                                    continue
                                else:
                                    # 最后一次尝试也失败
                                    logger.error(f"多次尝试后仍在登录页，登录状态可能已失效: {final_url}")
                                    with self._lock:
                                        self._status = "not_logged_in"
                                        self._last_error = "登录状态已失效，请重新登录"
                                    return self.get_login_status()
                            elif "dashboard" in final_url or "marketplace.emag.ro" in final_url:
                                # 成功到达 dashboard 或 marketplace 页面
                                logger.info(f"成功导航到目标页面: {final_url}")
                                break
                            else:
                                logger.warning(f"导航后到达未知页面: {final_url}")
                                break
                        except Exception as nav_err:
                            logger.warning(f"导航到 dashboard 失败 (第 {attempt + 1} 次): {nav_err}")
                            if attempt < max_retries - 1:
                                time.sleep(2)
                                continue
                            else:
                                logger.error(f"多次尝试导航都失败")
                
                page_info = self._collect_page_info(page)
                with self._lock:
                    self._status = "logged_in"
                    self._last_page_info = page_info
                    self._last_captcha_png_b64 = None
                    self._last_error = None
            return self.get_login_status()

            # 仍未登录：刷新截图给前端
            shot = self._screenshot_png_b64(page)
            with self._lock:
                if self._is_sms_verification_required(page):
                    self._status = "sms_verification_required"
                    self._last_error = "需要手机验证码"
                elif self._is_captcha_present(page):
                    self._status = "captcha_required"
                    self._last_error = None
                else:
                    self._status = "error"
                    self._last_error = "验证码确认后仍未登录成功，请检查是否账号/密码错误或仍有二次验证。"
                self._last_captcha_png_b64 = shot
            return self.get_login_status()

        except Exception as e:
            logger.exception("captcha_done check failed")
            with self._lock:
                self._status = "error"
                self._last_error = str(e)
            return self.get_login_status()

    def submit_sms_code(self, code: str) -> Dict[str, Any]:
        """
        提交手机验证码
        """
        with self._lock:
            if not self._page:
                self._status = "not_logged_in"
                self._last_error = "没有可用的浏览器会话"
                return self.get_login_status()

        try:
            page = self._page
            assert page is not None
            page.bring_to_front()

            # 查找验证码输入框
            code_selectors = [
                'input[type="tel"]',
                'input[name*="code" i]',
                'input[name*="verification" i]',
                'input[name*="otp" i]',
                'input[name*="sms" i]',
                'input[placeholder*="code" i]',
                'input[placeholder*="验证码" i]',
                'input[id*="code" i]',
                'input[id*="verification" i]',
            ]
            
            code_input = None
            for sel in code_selectors:
                try:
                    loc = page.locator(sel)
                    if loc.count() > 0:
                        code_input = loc.first
                        if code_input.is_visible():
                            break
                except Exception:
                    continue
            
            if not code_input:
                with self._lock:
                    self._status = "error"
                    self._last_error = "未找到验证码输入框"
                return self.get_login_status()
            
            # 填写验证码
            code_input.click()
            code_input.fill(code)
            logger.info(f"已填写手机验证码: {code}")
            
            # 查找提交按钮
            submit_selectors = [
                'button[type="submit"]',
                'button:has-text("Verify")',
                'button:has-text("验证")',
                'button:has-text("Submit")',
                'button:has-text("提交")',
                'button:has-text("Continue")',
                'button:has-text("继续")',
                'input[type="submit"]',
            ]
            
            submit_btn = None
            for sel in submit_selectors:
                try:
                    loc = page.locator(sel)
                    if loc.count() > 0:
                        submit_btn = loc.first
                        if submit_btn.is_visible():
                            break
                except Exception:
                    continue
            
            if submit_btn:
                submit_btn.click()
            else:
                # fallback：按 Enter
                page.keyboard.press("Enter")
            
            # 等待跳转/网络稳定
            time.sleep(1)
            self._wait_after_submit(page)
            
            # 检查是否需要手机验证码（验证码错误的情况）
            if self._is_sms_verification_required(page):
                shot = self._screenshot_png_b64(page)
                with self._lock:
                    self._status = "sms_verification_required"
                    self._last_captcha_png_b64 = shot
                    self._last_error = "验证码可能错误，请重新输入"
                return self.get_login_status()
            
            if self._is_logged_in(page):
                # 验证码确认后登录成功，等待一下让登录状态稳定，然后导航到 dashboard
                logger.info("手机验证码确认后登录成功，等待登录状态稳定...")
                time.sleep(2)  # 等待登录状态稳定
                
                # 再次检查登录状态
                if not self._is_logged_in(page):
                    logger.warning("等待后登录状态丢失，可能登录未完全成功")
                else:
                    # 导航到 dashboard（可能被重定向到登录页，需要再次导航）
                    max_retries = 3
                    for attempt in range(max_retries):
                        logger.info(f"尝试导航到 dashboard (第 {attempt + 1}/{max_retries} 次)")
                        try:
                            page.goto(self.DASHBOARD_URL, wait_until="domcontentloaded", timeout=15000)
                            time.sleep(2)  # 等待页面加载和可能的重定向
                            final_url = page.url
                            logger.info(f"导航完成，当前 URL: {final_url}")
                            
                            # 检查是否跳转到登录页
                            if "auth.emag" in final_url or "/login" in final_url:
                                logger.warning(f"导航后跳转到登录页: {final_url}")
                                if attempt < max_retries - 1:
                                    # 等待一下，然后再次尝试导航
                                    logger.info(f"等待 3 秒后再次尝试导航到 dashboard...")
                                    time.sleep(3)
                                    continue
                                else:
                                    # 最后一次尝试也失败
                                    logger.error(f"多次尝试后仍在登录页，登录状态可能已失效: {final_url}")
                                    with self._lock:
                                        self._status = "not_logged_in"
                                        self._last_error = "登录状态已失效，请重新登录"
                                    return self.get_login_status()
                            elif "dashboard" in final_url or "marketplace.emag.ro" in final_url:
                                # 成功到达 dashboard 或 marketplace 页面
                                logger.info(f"成功导航到目标页面: {final_url}")
                                break
                            else:
                                logger.warning(f"导航后到达未知页面: {final_url}")
                                break
                        except Exception as nav_err:
                            logger.warning(f"导航到 dashboard 失败 (第 {attempt + 1} 次): {nav_err}")
                            if attempt < max_retries - 1:
                                time.sleep(2)
                                continue
                            else:
                                logger.error(f"多次尝试导航都失败")
                
                page_info = self._collect_page_info(page)
                with self._lock:
                    self._status = "logged_in"
                    self._last_page_info = page_info
                    self._last_captcha_png_b64 = None
                    self._last_error = None
                return self.get_login_status()

            # 仍未登录：刷新截图给前端
            shot = self._screenshot_png_b64(page)
            with self._lock:
                if self._is_sms_verification_required(page):
                    self._status = "sms_verification_required"
                    self._last_error = "手机验证码可能错误，请重新输入"
                elif self._is_captcha_present(page):
                    self._status = "captcha_required"
                    self._last_error = None
                else:
                    self._status = "error"
                    self._last_error = "手机验证码提交后仍未登录成功，请检查验证码是否正确。"
                self._last_captcha_png_b64 = shot
            return self.get_login_status()

        except Exception as e:
            logger.exception("submit_sms_code failed")
            with self._lock:
                self._status = "error"
                self._last_error = str(e)
            return self.get_login_status()

    # ------------------------------------------------------------------ #
    # Session management
    # ------------------------------------------------------------------ #

    def _ensure_session(self) -> None:
        with self._lock:
            current_thread_id = threading.current_thread().ident
            # 如果已存在 context/page，且归属线程就是当前线程，可以安全复用
            if (
                self._context
                and self._page
                and self._owner_thread_id is not None
                and self._owner_thread_id == current_thread_id
            ):
                return

            # 如果存在 context/page 但归属线程与当前线程不同，必须销毁后重新创建，避免跨线程复用
            if (
                self._context
                and self._page
                and self._owner_thread_id is not None
                and self._owner_thread_id != current_thread_id
            ):
                self._close_session_locked()

            # 优先 BitBrowser（便于手动验证码），否则普通 Playwright
            window = None
            if getattr(config, "BITBROWSER_ENABLED", False):
                try:
                    window = bitbrowser_manager.acquire_exclusive_window(timeout=15)
                except Exception:
                    window = None

            if window and window.get("ws") and window.get("id"):
                self._window_id = window["id"]
                self._cdp_ws = window["ws"]
                self._context = self._pool.acquire_context(cdp_url=self._cdp_ws, window_id=self._window_id)
            else:
                self._window_id = None
                self._cdp_ws = None
                # 手动登录模式：强制 headless=False 以便用户手动登录
                # 检查是否已有独立的 browser 实例
                if self._browser_instance and self._playwright_instance:
                    try:
                        # 检查 browser 是否仍然连接
                        if self._browser_instance.is_connected():
                            logger.info("复用已有的浏览器实例")
                            # 检查是否有有效的 context
                            if not self._context:
                                # 尝试加载保存的登录状态
                                if self._auth_storage_path.exists():
                                    try:
                                        logger.info(f"正在加载保存的登录状态: {self._auth_storage_path}")
                                        self._context = self._browser_instance.new_context(
                                            storage_state=str(self._auth_storage_path),
                                        )
                                        logger.info("已加载保存的登录状态")
                                    except Exception as e:
                                        logger.warning(f"加载保存的登录状态失败: {e}，创建新 context")
                                        self._context = self._browser_instance.new_context()
                                else:
                                    self._context = self._browser_instance.new_context()
                        else:
                            logger.warning("已有浏览器实例已断开，需要重新创建")
                            # 清理旧的实例
                            try:
                                if self._browser_instance:
                                    self._browser_instance.close()
                            except Exception:
                                pass
                            try:
                                if self._playwright_instance:
                                    self._playwright_instance.stop()
                            except Exception:
                                pass
                            self._browser_instance = None
                            self._playwright_instance = None
                            raise Exception("浏览器已断开")
                    except Exception as e:
                        logger.warning(f"复用浏览器实例失败: {e}，将创建新实例")
                        # 清理并继续创建新实例
                        try:
                            if self._browser_instance:
                                self._browser_instance.close()
                        except Exception:
                            pass
                        try:
                            if self._playwright_instance:
                                self._playwright_instance.stop()
                        except Exception:
                            pass
                        self._browser_instance = None
                        self._playwright_instance = None
                
                # 如果没有有效的 browser 实例，创建新的（但只在第一次创建时）
                if not self._browser_instance or not self._playwright_instance:
                    logger.info("创建新的浏览器实例（手动登录模式，headless=False）")
                    # 注意：不能在 FastAPI 的异步环境中直接创建 sync_playwright
                    # 应该使用 pool 的方式，但我们需要 headless=False
                    # 这里我们使用一个变通方法：在 BackgroundTask 中运行，所以是同步的
                    # 但如果是在异步环境中调用，会失败
                    # 检查是否在异步环境中
                    try:
                        import asyncio
                        loop = asyncio.get_running_loop()
                        # 如果在异步环境中，不能创建 sync_playwright
                        logger.error("检测到在异步环境中，无法创建 sync_playwright 实例")
                        raise Exception("不能在异步环境中创建 sync_playwright 实例。请确保 login() 在 BackgroundTask 中运行。")
                    except RuntimeError:
                        # 没有运行中的事件循环，可以安全创建
                        pass
                    
                    from playwright.sync_api import sync_playwright
                    try:
                        logger.info("正在创建 Playwright 实例...")
                        playwright_instance = sync_playwright().start()
                        logger.info("Playwright 实例创建成功")
                    except Exception as e:
                        logger.exception(f"创建 Playwright 实例失败: {e}")
                        raise
                    
                    try:
                        logger.info("正在启动浏览器 (headless=False)...")
                        browser = playwright_instance.chromium.launch(headless=False)  # 强制 headless=False
                        logger.info("浏览器实例启动成功")
                    except Exception as e:
                        logger.exception(f"启动浏览器失败: {e}")
                        try:
                            playwright_instance.stop()
                        except Exception:
                            pass
                        raise
                    
                    # 尝试加载保存的登录状态
                    try:
                        if self._auth_storage_path.exists():
                            try:
                                logger.info(f"正在加载保存的登录状态: {self._auth_storage_path}")
                                self._context = browser.new_context(
                                    storage_state=str(self._auth_storage_path),
                                )
                                logger.info("已加载保存的登录状态")
                            except Exception as e:
                                logger.warning(f"加载保存的登录状态失败: {e}，创建新 context")
                                self._context = browser.new_context()
                                logger.info("已创建新的浏览器上下文")
                        else:
                            logger.info("没有保存的登录状态，创建新 context")
                            self._context = browser.new_context()
                            logger.info("已创建新的浏览器上下文")
                    except Exception as e:
                        logger.exception(f"创建浏览器上下文失败: {e}")
                        try:
                            browser.close()
                            playwright_instance.stop()
                        except Exception:
                            pass
                        raise
                    
                    # 保存 playwright 和 browser 实例，以便后续关闭
                    self._playwright_instance = playwright_instance
                    self._browser_instance = browser
                    logger.info("浏览器会话创建完成")

            # 创建一个长期存活的 page
            try:
                logger.info("正在创建浏览器页面...")
                if not self._context:
                    raise Exception("context 为 None，无法创建 page")
                self._page = self._context.new_page()
                logger.info(f"浏览器页面创建成功，page 对象: {self._page}")
                self._owner_thread_id = current_thread_id
            except Exception as e:
                logger.exception(f"创建浏览器页面失败: {e}")
                raise

    def _close_session_locked(self) -> None:
        # 注意：release_context 会在 CDP 模式下立即关闭 context 并断开连接
        try:
            if self._page:
                try:
                    self._page.close()
                except Exception:
                    pass
        finally:
            self._page = None

        try:
            if self._context:
                # 如果是手动登录模式（有独立的 browser 实例），直接关闭 browser
                if self._browser_instance:
                    try:
                        self._browser_instance.close()
                    except Exception:
                        pass
                    self._browser_instance = None
                # 如果有独立的 playwright 实例，停止它
                if self._playwright_instance:
                    try:
                        self._playwright_instance.stop()
                    except Exception:
                        pass
                    self._playwright_instance = None
                # 否则使用 pool 的 release_context
                if not self._browser_instance:
                    try:
                        self._pool.release_context(self._context)
                    except Exception:
                        try:
                            self._context.close()
                        except Exception:
                            pass
        finally:
            self._context = None

        # BitBrowser 窗口需要释放回池
        if self._window_id:
            try:
                bitbrowser_manager.release_window(self._window_id)
            except Exception:
                pass
        self._window_id = None
        self._cdp_ws = None

    # ------------------------------------------------------------------ #
    # Login flow helpers
    # ------------------------------------------------------------------ #

    # ------------------------------------------------------------------ #
    # Login form helpers
    # ------------------------------------------------------------------ #

    _USER_SELECTORS = [
        'input[name="email"]',
        'input[type="email"]',
        'input[name="username"]',
        'input[id*="email" i]',
        'input[placeholder*="email" i]',
        'input[placeholder*="e-mail" i]',
        'input[autocomplete="username"]',
    ]
    _PASS_SELECTORS = [
        'input[name="password"]',
        'input[type="password"]',
        'input[autocomplete="current-password"]',
    ]
    _SUBMIT_SELECTORS = [
        'button[type="submit"]',
        'button:has-text("Login")',
        'button:has-text("Sign in")',
        'button:has-text("Autentificare")',
        'button:has-text("Conectare")',
        'button:has-text("Next")',
        'button:has-text("Continue")',
        'input[type="submit"]',
    ]

    def _fill_login_form_multistep(self, page: Page, username: str, password: str) -> bool:
        """
        处理 eMAG 的多步登录表单：
        - 第 1 步：填写邮箱 → 点击提交/下一步
        - 第 2 步：填写密码 → 点击登录
        如果是单步表单（邮箱和密码同时可见），则一次填完。
        
        返回 True 表示表单提交成功（不代表已登录），False 表示找不到表单元素。
        """
        try:
            # 等待页面加载稳定
            try:
                page.wait_for_load_state("domcontentloaded", timeout=10000)
            except Exception:
                pass
            time.sleep(1)

            # 查找邮箱输入框
            user_el = self._first_visible(page, self._USER_SELECTORS)
            if not user_el:
                logger.warning("未找到邮箱/用户名输入框")
                return False

            # 查找密码输入框
            pass_el = self._first_visible(page, self._PASS_SELECTORS)

            if user_el and pass_el:
                # ── 单步表单：邮箱和密码同时可见 ──
                logger.info("检测到单步登录表单，同时填写邮箱和密码")
                user_el.click()
                user_el.fill(username)
                time.sleep(0.3)
                pass_el.click()
                pass_el.fill(password)
                time.sleep(0.3)

                btn = self._first_visible(page, self._SUBMIT_SELECTORS)
                if btn:
                    btn.click()
                else:
                    page.keyboard.press("Enter")
                logger.info("单步表单已提交")
                return True

            # ── 多步表单：只看到邮箱，密码还不可见 ──
            logger.info("检测到多步登录表单（仅邮箱可见），开始第 1 步")
            user_el.click()
            user_el.fill(username)
            time.sleep(0.3)

            # 第 1 步：提交邮箱
            btn = self._first_visible(page, self._SUBMIT_SELECTORS)
            if btn:
                btn.click()
            else:
                page.keyboard.press("Enter")
            logger.info("第 1 步：邮箱已提交，等待密码输入框出现...")

            # 等待密码输入框出现（最多等 10 秒）
            pass_el = None
            for attempt in range(20):
                time.sleep(0.5)
                pass_el = self._first_visible(page, self._PASS_SELECTORS)
                if pass_el:
                    logger.info(f"密码输入框在第 {attempt + 1} 次检查时出现")
                    break
                # 如果页面出现了 CAPTCHA 或错误提示，提前退出
                if self._is_captcha_present(page):
                    logger.info("第 1 步提交后出现 CAPTCHA")
                    return False
            
            if not pass_el:
                logger.warning("等待密码输入框超时（10 秒），多步登录失败")
                return False

            # 第 2 步：填写密码并提交
            logger.info("第 2 步：填写密码并提交")
            pass_el.click()
            pass_el.fill(password)
            time.sleep(0.3)

            btn = self._first_visible(page, self._SUBMIT_SELECTORS)
            if btn:
                btn.click()
            else:
                page.keyboard.press("Enter")
            logger.info("第 2 步：密码已提交")
            return True

        except Exception as e:
            logger.exception(f"多步登录表单填充失败: {e}")
            return False

    def _prefill_login_form(self, page: Page, username: str, password: str) -> None:
        """
        在弹窗 fallback 模式中预填表单（仅填充，不提交），方便用户手动检查后提交。
        """
        try:
            time.sleep(1)
            user_el = self._first_visible(page, self._USER_SELECTORS)
            if user_el:
                user_el.click()
                user_el.fill(username)
                logger.info("已预填邮箱")

            pass_el = self._first_visible(page, self._PASS_SELECTORS)
            if pass_el:
                pass_el.click()
                pass_el.fill(password)
                logger.info("已预填密码")
        except Exception as e:
            logger.warning(f"预填表单失败: {e}")

    def _fill_login_form(self, page: Page, username: str, password: str) -> None:
        """向后兼容的旧接口，内部调用多步方法。"""
        self._fill_login_form_multistep(page, username, password)

    def _wait_after_submit(self, page: Page) -> None:
        try:
            page.wait_for_load_state("domcontentloaded", timeout=15000)
        except Exception:
            pass
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

    def _is_captcha_present(self, page: Page) -> bool:
        try:
            # reCAPTCHA 常见 iframe / div
            candidates = [
                'iframe[title*="recaptcha" i]',
                'iframe[src*="recaptcha" i]',
                'div.g-recaptcha',
                'div:has-text("reCAPTCHA")',
            ]
            for sel in candidates:
                loc = page.locator(sel)
                if loc.count() > 0:
                    return True
        except Exception:
            return False
        return False

    def _is_sms_verification_required(self, page: Page) -> bool:
        """
        检测是否需要手机验证码（SMS验证码）
        """
        try:
            url = page.url or ""
            logger.info(f"[SMS检测] 开始检测 - 当前 URL: {url}")
            
            # 检查 URL 是否包含验证相关路径
            verification_keywords = ["verification", "verify", "sms", "code", "otp", "2fa", "two-factor", "twofactor", "authenticator"]
            url_lower = url.lower()
            for keyword in verification_keywords:
                if keyword in url_lower:
                    logger.info(f"[SMS检测] URL 包含验证相关关键词 '{keyword}': {url}")
                    return True
            
            # 如果 URL 是 auth.emag.net 但不是 /login，可能是验证码页面
            if "auth.emag.net" in url and "/login" not in url:
                logger.info(f"[SMS检测] 在 auth.emag.net 域名下但不在 /login 路径，可能是验证码页面: {url}")
                # 继续检查元素
            
            # 等待页面稳定
            try:
                page.wait_for_load_state("domcontentloaded", timeout=3000)
            except Exception:
                pass
            
            # 获取页面文本内容（用于调试）
            try:
                page_text = page.locator('body').inner_text(timeout=2000)
                page_text_lower = page_text.lower()
                # 检查页面文本中是否包含验证码相关关键词
                text_keywords = ["verification code", "verification", "enter code", "输入验证码", "sms code", "手机验证码", "two-factor", "2fa"]
                for keyword in text_keywords:
                    if keyword in page_text_lower:
                        logger.info(f"页面文本包含验证码关键词 '{keyword}'")
                        # 继续检查元素，确保不是误判
                        break
            except Exception as e:
                logger.debug(f"获取页面文本失败: {e}")
            
            # 检查页面元素
            sms_markers = [
                'input[type="tel"]',
                'input[type="text"][name*="code" i]',
                'input[type="text"][name*="verification" i]',
                'input[type="text"][name*="otp" i]',
                'input[type="text"][name*="sms" i]',
                'input[name*="code" i]',
                'input[name*="verification" i]',
                'input[name*="otp" i]',
                'input[name*="sms" i]',
                'input[placeholder*="code" i]',
                'input[placeholder*="验证码" i]',
                'input[placeholder*="verification" i]',
                'input[placeholder*="enter code" i]',
                'input[id*="code" i]',
                'input[id*="verification" i]',
                'input[id*="otp" i]',
                'input[id*="sms" i]',
            ]
            
            found_elements = []
            for sel in sms_markers:
                try:
                    loc = page.locator(sel)
                    count = loc.count()
                    if count > 0:
                        # 检查是否可见
                        for i in range(min(count, 3)):  # 最多检查前3个
                            try:
                                el = loc.nth(i)
                                if el.is_visible(timeout=500):
                                    found_elements.append(sel)
                                    logger.info(f"检测到手机验证码输入框: {sel} (可见)")
                                    break
                            except Exception:
                                continue
                except Exception as e:
                    logger.debug(f"检查选择器 {sel} 时出错: {e}")
                    continue
            
            # 检查文本元素
            text_markers = [
                'div:has-text("verification code")',
                'div:has-text("Verification Code")',
                'div:has-text("验证码")',
                'div:has-text("SMS")',
                'div:has-text("Enter code")',
                'div:has-text("输入验证码")',
                'div:has-text("Two-factor")',
                'div:has-text("2FA")',
                'label:has-text("verification code")',
                'label:has-text("验证码")',
                'span:has-text("verification code")',
                'span:has-text("验证码")',
            ]
            
            for sel in text_markers:
                try:
                    loc = page.locator(sel)
                    if loc.count() > 0:
                        found_elements.append(sel)
                        logger.info(f"检测到手机验证码文本元素: {sel}")
                        break
                except Exception:
                    continue
            
            if found_elements:
                logger.info(f"[SMS检测] 检测到需要手机验证码，找到的元素: {found_elements}")
                return True
            
            # 如果页面文本包含验证码相关关键词，也认为需要验证码
            try:
                page_text = page.locator('body').inner_text(timeout=2000)
                page_text_lower = page_text.lower()
                text_keywords = ["verification code", "enter code", "输入验证码", "sms code", "手机验证码", "two-factor", "2fa", "authenticator code"]
                for keyword in text_keywords:
                    if keyword in page_text_lower:
                        logger.info(f"[SMS检测] 页面文本包含验证码关键词 '{keyword}'")
                        return True
            except Exception as e:
                logger.debug(f"[SMS检测] 获取页面文本失败: {e}")
            
            logger.info(f"[SMS检测] 未检测到手机验证码要求 - URL: {url}")
            return False
            
        except Exception as e:
            logger.exception(f"检测手机验证码时出错: {e}")
            return False

    # 公共首页 URL 模式（未登录时跳转到的页面）
    _PUBLIC_PAGE_PATTERNS = ("/ro", "/bg", "/hu", "/pl")

    def _is_logged_in(self, page: Page) -> bool:
        """
        通过 URL + 页面元素双重判断已登录态（尽量稳健）。
        """
        # #region agent log
        import json as _dbg_j5
        _dbg_lp5 = r"d:\emag_erp\.cursor\debug.log"
        def _dbg_w5(loc, msg, data, hyp):
            try:
                import time as _t
                with open(_dbg_lp5, "a", encoding="utf-8") as _f:
                    _f.write(_dbg_j5.dumps({"timestamp": int(_t.time()*1000), "location": loc, "message": msg, "data": data, "hypothesisId": hyp}, ensure_ascii=False) + "\n")
            except Exception:
                pass
        # #endregion
        try:
            url = page.url or ""

            # 排除公共首页：marketplace.emag.ro/ro, /bg, /hu 等是未登录公共页面
            url_path = url.rstrip("/").split("marketplace.emag.ro")[-1] if "marketplace.emag.ro" in url else ""
            if url_path in self._PUBLIC_PAGE_PATTERNS:
                # #region agent log
                _dbg_w5("login_svc:_is_logged_in", "public page detected -> False", {"url": url, "url_path": url_path}, "H5")
                # #endregion
                return False

            if "marketplace.emag.ro/dashboard" in url:
                # #region agent log
                _dbg_w5("login_svc:_is_logged_in", "dashboard in URL -> True", {"url": url}, "H5")
                # #endregion
                return True
            # 有时会跳到别的后台页，只要不是 auth 域并且出现 dashboard 元素就算登录
            if "auth.emag" not in url and "marketplace.emag.ro" in url:
                # 常见左侧菜单或顶部账户菜单
                logged_in_markers = [
                    'a[href*="/dashboard"]',
                    'a:has-text("Dashboard")',
                    'button:has-text("Logout")',
                    'a:has-text("Logout")',
                ]
                for sel in logged_in_markers:
                    loc = page.locator(sel)
                    cnt = loc.count()
                    if cnt > 0:
                        # #region agent log
                        _dbg_w5("login_svc:_is_logged_in", "marker matched", {"url": url, "selector": sel, "count": cnt}, "H5")
                        # #endregion
                        return True
        except Exception:
            return False
        return False

    def _collect_page_info(self, page: Page) -> LoginPageInfo:
        url = ""
        title = ""
        seller_hint = None
        try:
            url = page.url or ""
        except Exception:
            url = ""
        try:
            title = page.title() or ""
        except Exception:
            title = ""
        try:
            # 尝试抓取页面上可能出现的卖家/账户信息（尽量不依赖具体结构）
            for sel in [
                '[data-testid*="account" i]',
                '[class*="account" i]',
                'header [class*="user" i]',
                'header [class*="profile" i]',
            ]:
                loc = page.locator(sel)
                if loc.count() > 0:
                    txt = loc.first.inner_text(timeout=800)
                    if txt:
                        seller_hint = " ".join(txt.split())[:120]
                        break
        except Exception:
            seller_hint = None
        return LoginPageInfo(url=url, title=title, seller_hint=seller_hint)

    def _screenshot_png_b64(self, page: Page) -> str:
        png_bytes = page.screenshot(full_page=True)
        return base64.b64encode(png_bytes).decode("ascii")

    def _first_visible(self, page: Page, selectors: list[str]):
        for sel in selectors:
            try:
                loc = page.locator(sel)
                if loc.count() > 0:
                    el = loc.first
                    try:
                        if el.is_visible(timeout=800):
                            return el
                    except Exception:
                        # 某些版本无 timeout 参数
                        if el.is_visible():
                            return el
            except Exception:
                continue
        return None

    def _create_authed_page(self):
        """
        创建一个已认证的 playwright 会话（参照 login_test.py）。
        使用保存的 storage_state 来恢复登录状态。
        
        Returns:
            (playwright_instance, browser, context, page) 元组
            调用方负责在使用完毕后关闭 browser 和 playwright_instance。
        """
        # #region agent log
        import json as _dbg_json
        _dbg_log_path = r"d:\emag_erp\.cursor\debug.log"
        def _dbg_write(loc, msg, data, hyp):
            try:
                import time as _t
                with open(_dbg_log_path, "a", encoding="utf-8") as _f:
                    _f.write(_dbg_json.dumps({"timestamp": int(_t.time()*1000), "location": loc, "message": msg, "data": data, "hypothesisId": hyp}, ensure_ascii=False) + "\n")
            except Exception:
                pass
        try:
            import os as _dbg_os
            _dbg_write(
                "login_svc:_create_authed_page:entry",
                "enter _create_authed_page",
                {
                    "dashboard_url": getattr(self, "DASHBOARD_URL", None),
                    "auth_storage_path": str(self._auth_storage_path),
                    "path_exists": self._auth_storage_path.exists(),
                    "current_shop_id": getattr(self, "_current_shop_id", None),
                    "env_proxy_keys_present": {
                        "HTTP_PROXY": bool(_dbg_os.environ.get("HTTP_PROXY")),
                        "HTTPS_PROXY": bool(_dbg_os.environ.get("HTTPS_PROXY")),
                        "NO_PROXY": bool(_dbg_os.environ.get("NO_PROXY")),
                    },
                },
                "S1",
            )
        except Exception:
            pass
        # #endregion

        if not self._auth_storage_path.exists():
            raise Exception("未找到保存的登录状态，请先登录")
        
        from playwright.sync_api import sync_playwright
        pw = sync_playwright().start()
        browser = pw.chromium.launch(headless=False)
        context = browser.new_context(storage_state=str(self._auth_storage_path))
        page = context.new_page()

        # 导航到 Dashboard（而非首页），已登录时会停留在 dashboard，未登录时会被重定向
        # #region agent log
        try:
            import time as _dbg_t2
            _t0 = _dbg_t2.monotonic()
            _dbg_write(
                "login_svc:_create_authed_page:pre_goto",
                "about to goto DASHBOARD_URL",
                {"target_url": getattr(self, "DASHBOARD_URL", None), "timeout_ms": 15000, "wait_until": "domcontentloaded"},
                "S2",
            )
        except Exception:
            _t0 = None
        # #endregion
        try:
            resp = page.goto(self.DASHBOARD_URL, wait_until="domcontentloaded", timeout=15000)
            # #region agent log
            try:
                import time as _dbg_t3
                _elapsed = int((_dbg_t3.monotonic() - _t0) * 1000) if _t0 is not None else None
                _dbg_write(
                    "login_svc:_create_authed_page:goto_ok",
                    "goto DASHBOARD_URL ok",
                    {
                        "elapsed_ms": _elapsed,
                        "response_status": (resp.status if resp else None),
                        "response_url": (resp.url if resp else None),
                        "page_url": (page.url or ""),
                    },
                    "S2",
                )
            except Exception:
                pass
            # #endregion
        except Exception as _goto_e:
            # #region agent log
            try:
                import time as _dbg_t4
                _elapsed = int((_dbg_t4.monotonic() - _t0) * 1000) if _t0 is not None else None
                _dbg_write(
                    "login_svc:_create_authed_page:goto_err",
                    "goto DASHBOARD_URL error",
                    {"elapsed_ms": _elapsed, "error_type": type(_goto_e).__name__, "error": str(_goto_e)[:300], "page_url": (page.url or "")},
                    "S3",
                )
            except Exception:
                pass
            # #endregion
            raise

        time.sleep(2)
        
        current_url = page.url or ""

        # 判断是否在 dashboard 或其他已登录的后台页面
        on_dashboard = "dashboard" in current_url
        on_auth_page = "auth.emag" in current_url or "/login" in current_url
        # 公共首页 /ro, /bg, /hu 等表示被踢出登录
        on_public_page = bool(current_url.rstrip("/").split("/")[-1] in ("ro", "bg", "hu") and "marketplace.emag" in current_url)

        logged_in = on_dashboard and not on_auth_page and not on_public_page

        if not logged_in:
            # 登录状态已失效
            browser.close()
            pw.stop()
            with self._lock:
                self._status = "not_logged_in"
            # 删除过期的 auth 文件，避免反复使用失效 cookie
            if self._auth_storage_path.exists():
                try:
                    self._auth_storage_path.unlink()
                    logger.info(f"已删除过期的登录状态文件: {self._auth_storage_path}")
                except Exception:
                    pass
            raise Exception(
                f"保存的登录状态已失效（页面被重定向到: {current_url}），请重新登录"
            )
        
        logger.info(f"已创建认证会话，当前 URL: {page.url}")
        return pw, browser, context, page

    def fetch_inbound_shipments_all_pages(
        self,
        limit: int = 50,
        max_pages: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        获取所有页的入仓运单列表（自动翻页），并提取所有 id。
        每次调用会创建独立的 playwright 会话（参照 login_test.py），避免跨线程问题。
        """
        all_items = []
        all_ids = []
        current_page = 0
        total_items = None
        total_pages = None
        
        pw, browser, context, page_obj = None, None, None, None
        try:
            pw, browser, context, page_obj = self._create_authed_page()
            
            while True:
                if max_pages is not None and current_page >= max_pages:
                    logger.info(f"达到最大页数限制 {max_pages}，停止翻页")
                    break
                
                request_body = {
                    "sort_by": "id",
                    "sort_order": "desc",
                    "page": current_page + 1,
                    "rows": limit
                }
                
                logger.info(f"正在获取第 {current_page + 1} 页...")
                
                result = page_obj.evaluate(f"""
                    async () => {{
                        try {{
                            const res = await fetch('https://marketplace.emag.ro/api-ui/fio/reception/list', {{
                                method: 'POST',
                                headers: {{ 
                                    'content-type': 'application/json',
                                    'x-requested-with': 'XMLHttpRequest'
                                }},
                                body: JSON.stringify({json.dumps(request_body)})
                            }});
                            return await res.json();
                        }} catch (e) {{
                            return "ERROR_JS_" + e.message;
                        }}
                    }}
                """)
                
                # 检查 JS 错误
                if isinstance(result, str) and result.startswith("ERROR_JS_"):
                    raise RuntimeError(f"Fetch 请求失败: {result}")
                
                # 解析数据（与 login_test.py 一致）
                if not isinstance(result, dict) or "data" not in result:
                    logger.error(f"API 返回格式错误: {result}")
                    break
                
                data = result.get("data", {})
                rows = data.get("rows", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
                
                if not rows:
                    logger.info(f"第 {current_page + 1} 页无数据，停止翻页")
                    break
                
                page_ids = [item.get("id") for item in rows if item.get("id") is not None]
                all_ids.extend(page_ids)
                all_items.extend(rows)
                
                if total_items is None and isinstance(data, dict):
                    total_items = data.get("total", len(rows))
                    if total_items and limit:
                        total_pages = (total_items + limit - 1) // limit
                
                logger.info(f"已获取第 {current_page + 1} 页，{len(rows)} 条记录，累计 {len(all_items)} 条")
                
                if len(rows) < limit:
                    logger.info(f"第 {current_page + 1} 页记录数 < limit，已到最后一页")
                    break
                
                if total_pages and current_page + 1 >= total_pages:
                    logger.info(f"已获取所有 {total_pages} 页数据")
                    break
                
                current_page += 1
                time.sleep(0.5)
            
            ids_string = ", ".join(map(str, all_ids))
            logger.info(f"入仓运单同步完成：共 {len(all_ids)} 条，ID 列表: {ids_string}")
            
            result = {
                "all_ids": all_ids,
                "ids_string": ids_string,
                "items": all_items,
                "total_pages": total_pages or (current_page + 1),
                "total_items": total_items or len(all_items),
                "pages_fetched": current_page + 1
            }
            
            with self._lock:
                self._last_sync_result = result
                self._sync_status = "completed"
            
            return result
            
        except Exception as e:
            logger.exception("自动翻页获取入仓运单列表失败")
            ids_string = ", ".join(map(str, all_ids)) if all_ids else ""
            result = {
                "all_ids": all_ids,
                "ids_string": ids_string,
                "items": all_items,
                "total_pages": total_pages,
                "total_items": total_items or len(all_items),
                "pages_fetched": current_page + 1,
                "error": str(e)
            }
            with self._lock:
                self._last_sync_result = result
                self._sync_status = "error"
            return result
        finally:
            # 关闭独立会话
            try:
                if browser:
                    browser.close()
            except Exception:
                pass
            try:
                if pw:
                    pw.stop()
            except Exception:
                pass

    def sync_finalized_shipments_to_db(self, db: Session, limit: int = 50) -> Dict[str, Any]:
        """
        同步 finalized 运单详情到数据库（参照 login_test.py 的完整流程）。
        创建独立的 playwright 会话，避免跨线程问题。
        """
        pw, browser, context, page_obj = None, None, None, None
        try:
            pw, browser, context, page_obj = self._create_authed_page()
            
            # #region agent log
            import json as _dbg_json2
            _dbg_log_path2 = r"d:\emag_erp\.cursor\debug.log"
            def _dbg_write2(loc, msg, data, hyp):
                try:
                    import time as _t
                    with open(_dbg_log_path2, "a", encoding="utf-8") as _f:
                        _f.write(_dbg_json2.dumps({"timestamp": int(_t.time()*1000), "location": loc, "message": msg, "data": data, "hypothesisId": hyp}, ensure_ascii=False) + "\n")
                except Exception:
                    pass
            _dbg_write2("login_svc:sync_shipments:page_ready", "page state before fetch", {"url": page_obj.url, "title": page_obj.title()}, "H2,H3")
            # #endregion

            # 1. 获取运单列表
            logger.info("正在获取运单列表...")
            list_payload = {
                "sort_by": "id",
                "sort_order": "desc",
                "page": 1,
                "rows": limit
            }
            
            # #region agent log
            _dbg_write2("login_svc:sync_shipments:before_fetch", "about to call JS fetch", {"page_url": page_obj.url}, "H3,H4")
            # #endregion

            list_res = page_obj.evaluate(f"""
                async () => {{
                    try {{
                        const res = await fetch('https://marketplace.emag.ro/api-ui/fio/reception/list', {{
                            method: 'POST',
                            headers: {{ 
                                'content-type': 'application/json',
                                'x-requested-with': 'XMLHttpRequest' 
                            }},
                            body: JSON.stringify({json.dumps(list_payload)})
                        }});
                        const status = res.status;
                        const text = await res.text();
                        try {{ return JSON.parse(text); }} catch(pe) {{ return "ERROR_JS_HTTP" + status + "_" + text.substring(0, 500); }}
                    }} catch (e) {{
                        return "ERROR_JS_" + e.message;
                    }}
                }}
            """)
            
            # #region agent log
            _dbg_write2("login_svc:sync_shipments:after_fetch", "fetch result", {"type": type(list_res).__name__, "is_error": isinstance(list_res, str) and str(list_res).startswith("ERROR_JS_"), "preview": str(list_res)[:500]}, "H2,H3,H4")
            # #endregion

            if isinstance(list_res, str) and list_res.startswith("ERROR_JS_"):
                raise RuntimeError(f"获取运单列表失败: {list_res}")
            
            if not isinstance(list_res, dict) or "data" not in list_res:
                raise RuntimeError(f"运单列表返回格式错误: {list_res}")
            
            # 2. 筛选除 draft, canceled 外的运单
            data = list_res.get("data", {})
            all_receptions = data.get("rows", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
            
            # 过滤掉 draft 和取消相关的状态
            target_list = [
                item for item in all_receptions 
                if item.get("status", "").lower() not in ("draft", "canceled", "cancelled", "anulat")
            ]
            
            logger.info(f"获取到 {len(all_receptions)} 条总记录，其中 {len(target_list)} 条为需要同步的运单")
            
            if not target_list:
                logger.info("没有找到需要同步的非取消、非草稿状态运单")
                return {
                    "success": True,
                    "total_finalized": 0,  # 保持原字段名兼容
                    "synced": 0,
                    "skipped": 0,
                    "errors": 0,
                    "error_list": []
                }
            
            synced_count = 0
            skipped_count = 0
            error_count = 0
            error_list = []
            
            # 3. 遍历目标运单，获取详情并存入数据库
            for item in target_list:
                reception_id = item.get("id")
                if not reception_id:
                    continue
                
                try:
                    new_status = item.get("status", "")
                    number_of_units = item.get("numberOfUnits", 0)  # 从列表 JSON 获取该字段
                    
                    # 检查是否已存在
                    existing = db.query(EmagInboundShipment).filter(
                        EmagInboundShipment.reception_id == reception_id
                    ).first()
                    
                    if existing:
                        # 如果原先和现在都是 finalized，且数量等没变，跳过避免重复抓取详情
                        if existing.status == "finalized" and new_status == "finalized" and existing.number_of_units == number_of_units:
                            logger.debug(f"运单 {reception_id} 已存在且已完成，跳过")
                            skipped_count += 1
                            continue
                        
                        logger.info(f"运单 {reception_id} 更新：状态 {existing.status}->{new_status}, 数量 -> {number_of_units}")
                        existing.status = new_status
                        existing.number_of_units = number_of_units
                        existing.synced_at = datetime.utcnow()
                        shipment = existing
                        
                        # 清除旧详情，后续重新插入（状态或数量有变动时需要刷新）
                        db.query(EmagInboundShipmentDetail).filter(
                            EmagInboundShipmentDetail.shipment_id == shipment.id
                        ).delete()
                    else:
                        # 写入运单主记录
                        shipment = EmagInboundShipment(
                            shop_id=self._current_shop_id,
                            reception_id=reception_id,
                            status=new_status,
                            number_of_units=number_of_units,
                            synced_at=datetime.utcnow()
                        )
                        db.add(shipment)
                    
                    db.flush()
                    
                    # 获取详情（已入仓数量或申请入库数量）
                    logger.info(f"正在获取运单 {reception_id} 的详情...")
                    
                    if new_status in ("approved", "in_transit"):
                        # 对于 approved 和 in_transit 状态，使用 header + line/list 接口抓取申请详情
                        js_fetch_lines = f"""
                            async () => {{
                                try {{
                                    // 1. 获取 Header
                                    const hRes = await fetch('https://marketplace.emag.ro/api-ui/fio/get-reception-header/{reception_id}', {{
                                        method: 'GET',
                                        headers: {{ 'x-requested-with': 'XMLHttpRequest' }}
                                    }});
                                    const hData = await hRes.json();
                                    const reservations = (hData && hData.data && hData.data.reservations) ? hData.data.reservations : [];
                                    
                                    let allLines = [];
                                    // 2. 遍历 reservation 获取 lines
                                    for (let resv of reservations) {{
                                        let page = 1;
                                        let pageSize = 100;
                                        let hasMore = true;
                                        while (hasMore) {{
                                            const lRes = await fetch('https://marketplace.emag.ro/api-ui/fio/reception-line/list', {{
                                                method: 'POST',
                                                headers: {{
                                                    'content-type': 'application/json',
                                                    'x-requested-with': 'XMLHttpRequest'
                                                }},
                                                body: JSON.stringify({{
                                                    reception_id: {reception_id},
                                                    reservation_id: resv.id,
                                                    page: page,
                                                    pageSize: pageSize
                                                }})
                                            }});
                                            const lData = await lRes.json();
                                            let rows = [];
                                            if (lData && lData.data) {{
                                                rows = lData.data.rows || lData.data;
                                            }}
                                            if (!Array.isArray(rows)) rows = [];
                                            
                                            allLines = allLines.concat(rows);
                                            if (rows.length < pageSize) {{
                                                hasMore = false;
                                            }} else {{
                                                page++;
                                            }}
                                        }}
                                    }}
                                    // 伪装成相同的返回格式，以便 Python 端统一处理
                                    return {{ "code": 200, "data": allLines }};
                                }} catch(e) {{
                                    return "ERROR_DETAIL_JS_" + e.message;
                                }}
                            }}
                        """
                        detail_data = page_obj.evaluate(js_fetch_lines)
                    else:
                        # 对于 finalized / receiving 等状态，继续使用原有的已上架数量接口
                        detail_url = f"https://marketplace.emag.ro/api-ui/fio/get-transferred-to-storage-quantity/{reception_id}"
                        detail_data = page_obj.evaluate(f"""
                            async () => {{
                                try {{
                                    const res = await fetch('{detail_url}', {{
                                        method: 'GET',
                                        headers: {{ 'x-requested-with': 'XMLHttpRequest' }}
                                    }});
                                    return await res.json();
                                }} catch (e) {{
                                    return "ERROR_DETAIL_JS_" + e.message;
                                }}
                            }}
                        """)
                    
                    if isinstance(detail_data, str) and detail_data.startswith("ERROR_DETAIL_JS_"):
                        raise RuntimeError(f"获取运单详情失败: {detail_data}")
                    
                    if not isinstance(detail_data, dict) or detail_data.get("code") != 200:
                        logger.warning(f"运单 {reception_id} 详情返回格式异常（可能尚未入仓）: {detail_data}")
                        db.commit()  # 正在处理中的运单可能接口尚无详情数据，视为成功同步了其"主状态"
                        synced_count += 1
                        continue
                    
                    detail_items = detail_data.get("data", [])
                    if not detail_items:
                        logger.warning(f"运单 {reception_id} 没有详情数据")
                        db.commit()  # 同理，没有详情依然算同步主表成功
                        synced_count += 1
                        continue
                    
                    for detail_item in detail_items:
                        expiration_date = None
                        # 兼容不同接口返回的过期时间字段名
                        exp_date_str = detail_item.get("expirationDate") or detail_item.get("expiration_date")
                        if exp_date_str:
                            try:
                                expiration_date = datetime.strptime(exp_date_str, "%Y-%m-%d").date()
                            except (ValueError, TypeError):
                                pass
                        
                        # 兼容不同接口返回的 商品ID 字段名
                        vid = detail_item.get("vendorProductId") or detail_item.get("vendor_product_id") or detail_item.get("productId") or detail_item.get("id")
                        
                        # 兼容不同接口返回的 数量 字段名（新接口大概率叫 quantity 或 expectedQuantity）
                        qty = detail_item.get("transferredToStorageQuantity") or detail_item.get("quantity") or detail_item.get("expectedQuantity") or detail_item.get("requestedQuantity") or 0
                        
                        # 兼容不同接口返回的 批次号 字段名
                        lot = detail_item.get("producerLot") or detail_item.get("producer_lot")
                        
                        detail = EmagInboundShipmentDetail(
                            shipment_id=shipment.id,
                            reception_id=reception_id,
                            vendor_product_id=vid,
                            transferred_to_storage_quantity=qty,
                            expiration_date=expiration_date,
                            producer_lot=lot,
                            synced_at=datetime.utcnow()
                        )
                        db.add(detail)
                    
                    db.commit()
                    synced_count += 1
                    logger.info(f"运单 {reception_id} 同步成功，共 {len(detail_items)} 条详情")
                    
                    time.sleep(1)
                    
                except Exception as e:
                    db.rollback()
                    logger.exception(f"同步运单 {reception_id} 失败: {e}")
                    error_count += 1
                    error_list.append({"reception_id": reception_id, "error": str(e)})
                    continue
            
            result = {
                "success": True,
                "total_finalized": len(target_list),
                "synced": synced_count,
                "skipped": skipped_count,
                "errors": error_count,
                "error_list": error_list
            }
            
            logger.info(f"运单详情同步完成: 总计 {len(target_list)} 条，成功 {synced_count} 条，跳过 {skipped_count} 条，失败 {error_count} 条")
            return result
            
        except Exception as e:
            logger.exception("同步运单详情到数据库失败")
            raise
        finally:
            try:
                if browser:
                    browser.close()
            except Exception:
                pass
            try:
                if pw:
                    pw.stop()
            except Exception:
                pass


emag_marketplace_login_service = EmagMarketplaceLoginService()


