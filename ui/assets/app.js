"use strict";
(() => {
  const bridge = window.timingBridge, form = document.querySelector("#config"), fields = document.querySelector("#fields"), message = document.querySelector("#message"), stats = document.querySelector("#stats");
  const defaults = { mode: "off", hint_after_ms: 1000, hint_wait_mode: "fixed", hint_min_ms: 1000, hint_max_ms: 2000, delay_ms: 1000, max_event_bytes: 262144, max_concurrent_transforms: 128, max_proxy_pools: 64, idle_connections_per_pool: 8, idle_timeout_seconds: 90, dial_timeout_seconds: 30, tls_handshake_timeout_seconds: 10, response_header_timeout_seconds: 0, enable_http2: true, retry_max_attempts: 2, retry_body_max_bytes: 2097152, retry_max_concurrent: 32, retry_backoff_ms: 500, retry_total_wait_ms: 10000 };
  let dirty = false, timer, observer, statusBusy = false, loadedConfig = { ...defaults };
  const show = (text, error = false) => { message.textContent = text; message.className = error ? "error" : ""; };
  const modeNote = () => {
    const mode = form.elements.namedItem("mode").value, ms = Number(form.elements.namedItem("hint_after_ms").value);
    const random = form.elements.namedItem("hint_wait_mode").value === "random";
    const min = form.elements.namedItem("hint_min_ms"), max = form.elements.namedItem("hint_max_ms");
    for (const [id, active] of [["hint-wait-field", mode === "hint"], ["hint-field", mode === "hint" && !random], ["hint-min-field", mode === "hint" && random], ["hint-max-field", mode === "hint" && random], ["delay-field", mode === "delay"]]) {
      const label = document.getElementById(id); label.hidden = !active;
      label.querySelector("input,select").disabled = !active;
    }
    min.setCustomValidity(mode === "hint" && random && Number(min.value) > Number(max.value) ? "随机等待下限须小于或等于上限" : "");
    const waitNote = random ? `每次请求从 ${min.value}–${max.value} 毫秒中均匀抽取一次等待阈值` : `等待 ${ms} 毫秒`;
    document.querySelector("#mode-note").textContent = {
      off: "当前原样传递。选择“超时空提示”并保存后，才会提前回应。",
      hint: !random && ms === 0 ? "旧版方式：等到上游真实空内容项，再追加一次空 delta；这个值不会启动等待计时器。提示发送成功后释放处理名额。" : `${waitNote}：期间收到首个有效模型输出，就正常传递；仍在等待时发送一次空提示，提前下游 semantic 首事件计时。提示或有效输出到达后释放名额，后续回答继续传输。`,
      delay: "首个输出事件到达后，额外等待指定时长再传递。此模式用于增加计时。"
    }[mode];
  };
  const fill = cfg => { loadedConfig = { ...defaults, ...cfg }; for (const [key, value] of Object.entries(loadedConfig)) { const el = form.elements.namedItem(key); if (!el) continue; if (el.type === "checkbox") el.checked = value; else el.value = value; } dirty = false; modeNote(); };
  // Preserve saved inactive settings, rather than submitting hidden invalid edits.
  const read = () => ({ ...loadedConfig, ...Object.fromEntries([...form.elements].filter(el => el.name && !el.disabled).map(el => [el.name, el.type === "checkbox" ? el.checked : el.type === "number" ? Number(el.value) : el.value])) });
  const load = async () => { if (dirty) { show("有未保存修改；请先保存，或重新打开配置页放弃修改", true); return; } const d = await bridge.request("config.load"); fill(d.config); show("配置已读取"); };
  const status = async () => {
    if (statusBusy) return;
    statusBusy = true;
    try {
      const d = await bridge.request("plugin.status"), r = d.result || {};
      let v = {}; try { v = JSON.parse(r.status_json || "{}"); } catch (_) {}
      stats.textContent = `${r.message || ""}\n${JSON.stringify(v, null, 2)}`;
      const mode = { off: "原样传递", hint: "超时空提示", delay: "增加延迟" }[v.mode] || "等待状态";
      const wait = v.hint_wait_mode === "random" ? `随机 ${v.hint_min_ms}–${v.hint_max_ms} 毫秒` : `固定 ${v.hint_after_ms ?? "—"} 毫秒`;
      document.querySelector("#status-note").textContent = `${r.healthy ? "运行中" : (r.message || "状态待确认")} · ${mode}${v.mode === "hint" ? ` · ${wait}` : ""}`;
      const summary = document.querySelector("#summary"); summary.replaceChildren();
      for (const [label, key] of [["超时提前提示", "timed_hints_sent"], ["期限内正常返回", "fast_responses"], ["等待提示或输出", "waiting_first_output"], ["占用处理名额", "active_transforms"], ["在途请求", "active_requests"], ["并发跳过", "transform_busy"], ["已发起额外重试", "retry_attempts"], ["重试后开始透传", "retry_streams_resumed"], ["重试次数耗尽", "retry_exhausted"], ["重试等待超预算", "retry_budget_exceeded"], ["重试资源满额跳过", "retry_capacity_bypassed"], ["占用重试缓冲", "active_retry_captures"]]) {
        const card = document.createElement("div"), title = document.createElement("span"), value = document.createElement("strong");
        title.textContent = label; value.textContent = Number.isFinite(v[key]) ? v[key].toLocaleString() : "—"; card.append(title, value); summary.append(card);
      }
    } finally { statusBusy = false; }
  };
  const run = async fn => { fields.disabled = true; try { await fn(); } catch (e) { show(e.message, true); } finally { fields.disabled = false; } };
  const changed = () => { dirty = true; modeNote(); show("有未保存修改"); };
  form.addEventListener("input", changed);
  form.addEventListener("submit", e => e.preventDefault());
  document.querySelector("#preset").onclick = () => { form.elements.namedItem("mode").value = "hint"; form.elements.namedItem("hint_wait_mode").value = "fixed"; form.elements.namedItem("hint_after_ms").value = "1000"; changed(); };
  document.querySelector("#save").onclick = () => { if (!form.reportValidity()) return; const cfg = read(); run(async () => { const d = await bridge.request("config.save", { config: cfg }); fill(d.config); show("配置已保存并应用"); await status(); }); };
  document.querySelector("#reload").onclick = () => run(load);
  document.querySelector("#test").onclick = () => run(async () => { if (dirty) throw new Error("请先保存修改，再检查"); const d = await bridge.request("config.test"); show(d.result?.message || "检查完成", !d.result?.success); });
  document.querySelector("#status").onclick = () => run(status);
  addEventListener("beforeunload", e => { if (dirty) { e.preventDefault(); e.returnValue = ""; } });
  addEventListener("pagehide", () => { clearInterval(timer); observer?.disconnect(); bridge.dispose(); });
  (async () => { try { bridge.notify("sub2api.plugin.ready"); await load(); fields.disabled = false; await status(); timer = setInterval(() => status().catch(() => {}), 10000); observer = new ResizeObserver(() => bridge.notify("ui.resize", { height: Math.min(2400, document.documentElement.scrollHeight) })); observer.observe(document.body); } catch (e) { show(e.message, true); } })();
})();
