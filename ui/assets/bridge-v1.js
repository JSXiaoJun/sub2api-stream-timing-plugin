"use strict";
(() => {
  const token = new URLSearchParams(location.hash.slice(1)).get("bridge_token");
  const pending = new Map(); let sequence = 0; let disposed = false;
  const send = (type, data = {}) => {
    if (!token || parent === window || disposed) throw new Error("请从 Sub2API 插件配置页打开");
    const request_id = `timing-${Date.now()}-${++sequence}`;
    parent.postMessage({ ...data, source: "sub2api-plugin-ui", bridge_token: token, type, request_id }, "*");
    return request_id;
  };
  const onMessage = event => {
    const d = event.data;
    if (event.source !== parent || !d || d.source !== "sub2api-plugin-host" || d.bridge_token !== token) return;
    const p = pending.get(d.request_id); if (!p) return;
    clearTimeout(p.timer); pending.delete(d.request_id);
    if (d.ok) p.resolve(d); else p.reject(new Error(d.error?.message || (typeof d.error === "string" ? d.error : "") || d.result?.message || d.message || "宿主操作失败"));
  };
  addEventListener("message", onMessage);
  window.timingBridge = {
    request(type, data = {}) {
      return new Promise((resolve, reject) => {
        let id; try { id = send(type, data); } catch (e) { reject(e); return; }
        const timer = setTimeout(() => { pending.delete(id); reject(new Error("操作超时，请检查宿主状态")); }, 30000);
        pending.set(id, { resolve, reject, timer });
      });
    },
    notify: send,
    dispose() { disposed = true; removeEventListener("message", onMessage); for (const p of pending.values()) { clearTimeout(p.timer); p.reject(new Error("页面已关闭")); } pending.clear(); }
  };
})();
