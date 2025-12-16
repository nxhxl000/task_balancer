import React, { useEffect, useMemo, useState } from "react";

type TaskStatus = "queued" | "leased" | "running" | "done" | "failed" | "canceled";

type Task = {
  id: string;
  task_type: string;
  status: TaskStatus;

  n: number;
  priority: number;
  attempts: number;
  max_attempts: number;

  leased_by?: string | null;
  lease_expires_at?: string | null;

  payload: Record<string, unknown>;
  result?: Record<string, unknown> | null;
  error?: string | null;

  created_at: string;
  updated_at: string;
};

type Order = "created_at_desc" | "created_at_asc" | "priority_desc";

const API_BASE = (import.meta as any).env?.VITE_TASK_API_URL?.replace(/\/$/, "") || "http://127.0.0.1:8000";

function shortId(id: string) {
  return id.length > 12 ? `${id.slice(0, 8)}…${id.slice(-4)}` : id;
}

function formatDt(s?: string | null) {
  if (!s) return "—";
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return s;
  return d.toLocaleString();
}

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const resp = await fetch(`${API_BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...(init?.headers || {}) },
    ...init,
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`HTTP ${resp.status}: ${text}`);
  }
  return (await resp.json()) as T;
}

export default function TasksView() {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  // filters
  const [status, setStatus] = useState<TaskStatus | "">("");
  const [taskType, setTaskType] = useState("");
  const [n, setN] = useState<string>("");

  // paging
  const [limit, setLimit] = useState(50);
  const [offset, setOffset] = useState(0);
  const [order, setOrder] = useState<Order>("created_at_desc");

  // UI
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [autoRefresh, setAutoRefresh] = useState(false);

  const query = useMemo(() => {
    const p = new URLSearchParams();
    if (status) p.set("status", status);
    if (taskType.trim()) p.set("task_type", taskType.trim());
    if (n.trim()) p.set("n", n.trim());
    p.set("limit", String(limit));
    p.set("offset", String(offset));
    p.set("order", order);
    return p.toString();
  }, [status, taskType, n, limit, offset, order]);

  async function load() {
    setLoading(true);
    setErr(null);
    try {
      const data = await api<Task[]>(`/tasks?${query}`);
      setTasks(data);
    } catch (e: any) {
      setErr(e?.message || String(e));
    } finally {
      setLoading(false);
    }
  }

  async function cancelTask(taskId: string) {
    try {
      await api(`/tasks/${taskId}/cancel`, { method: "POST" });
      await load();
    } catch (e: any) {
      alert(e?.message || String(e));
    }
  }

  function toggleExpand(taskId: string) {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(taskId)) next.delete(taskId);
      else next.add(taskId);
      return next;
    });
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [query]);

  useEffect(() => {
    if (!autoRefresh) return;
    const t = setInterval(() => load(), 3000);
    return () => clearInterval(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoRefresh, query]);

  return (
    <div style={{ padding: 16, maxWidth: 1200, margin: "0 auto", fontFamily: "system-ui, Arial" }}>
      <h2 style={{ marginTop: 0 }}>Tasks</h2>

      {/* Controls */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "160px 1fr 120px 160px 120px 120px",
          gap: 8,
          alignItems: "end",
          marginBottom: 12,
        }}
      >
        <div>
          <label style={{ display: "block", fontSize: 12, opacity: 0.8 }}>Status</label>
          <select value={status} onChange={(e) => setStatus(e.target.value as any)} style={{ width: "100%" }}>
            <option value="">All</option>
            <option value="queued">queued</option>
            <option value="leased">leased</option>
            <option value="running">running</option>
            <option value="done">done</option>
            <option value="failed">failed</option>
            <option value="canceled">canceled</option>
          </select>
        </div>

        <div>
          <label style={{ display: "block", fontSize: 12, opacity: 0.8 }}>Task type</label>
          <input
            value={taskType}
            onChange={(e) => setTaskType(e.target.value)}
            placeholder="latin_square_from_prefix"
            style={{ width: "100%" }}
          />
        </div>

        <div>
          <label style={{ display: "block", fontSize: 12, opacity: 0.8 }}>n</label>
          <input
            value={n}
            onChange={(e) => setN(e.target.value)}
            placeholder="5"
            style={{ width: "100%" }}
          />
        </div>

        <div>
          <label style={{ display: "block", fontSize: 12, opacity: 0.8 }}>Order</label>
          <select value={order} onChange={(e) => setOrder(e.target.value as Order)} style={{ width: "100%" }}>
            <option value="created_at_desc">created_at ↓</option>
            <option value="created_at_asc">created_at ↑</option>
            <option value="priority_desc">priority ↓</option>
          </select>
        </div>

        <div>
          <label style={{ display: "block", fontSize: 12, opacity: 0.8 }}>Limit</label>
          <input
            type="number"
            value={limit}
            min={1}
            max={500}
            onChange={(e) => setLimit(Number(e.target.value))}
            style={{ width: "100%" }}
          />
        </div>

        <div style={{ display: "flex", gap: 8 }}>
          <button onClick={() => load()} disabled={loading} style={{ padding: "6px 10px" }}>
            {loading ? "Loading…" : "Refresh"}
          </button>
          <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, opacity: 0.9 }}>
            <input type="checkbox" checked={autoRefresh} onChange={(e) => setAutoRefresh(e.target.checked)} />
            auto
          </label>
        </div>
      </div>

      {/* Paging */}
      <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 8 }}>
        <button onClick={() => setOffset((x) => Math.max(0, x - limit))} disabled={offset === 0}>
          ← Prev
        </button>
        <button onClick={() => setOffset((x) => x + limit)} disabled={tasks.length < limit}>
          Next →
        </button>
        <div style={{ fontSize: 12, opacity: 0.8 }}>offset: {offset}</div>
      </div>

      {err && (
        <div style={{ padding: 10, border: "1px solid #f5a", background: "#fff5f8", marginBottom: 10 }}>
          <b>Error:</b> {err}
          <div style={{ fontSize: 12, opacity: 0.8 }}>API: {API_BASE}</div>
        </div>
      )}

      {/* Table */}
      <div style={{ border: "1px solid #ddd", borderRadius: 8, overflow: "hidden" }}>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
            <thead style={{ background: "#f7f7f7" }}>
              <tr>
                <th style={th}>Created</th>
                <th style={th}>ID</th>
                <th style={th}>Type</th>
                <th style={th}>Status</th>
                <th style={th}>n</th>
                <th style={th}>Priority</th>
                <th style={th}>Attempts</th>
                <th style={th}>Leased by</th>
                <th style={th}>Lease expires</th>
                <th style={th}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {tasks.map((t) => {
                const isExpanded = expanded.has(t.id);
                const isFinal = t.status === "done" || t.status === "failed" || t.status === "canceled";
                return (
                  <React.Fragment key={t.id}>
                    <tr
                      onClick={() => toggleExpand(t.id)}
                      style={{
                        cursor: "pointer",
                        background: isExpanded ? "#fafafa" : "white",
                      }}
                    >
                      <td style={td}>{formatDt(t.created_at)}</td>
                      <td style={td} title={t.id}>{shortId(t.id)}</td>
                      <td style={td}>{t.task_type}</td>
                      <td style={td}><StatusPill status={t.status} /></td>
                      <td style={td}>{t.n}</td>
                      <td style={td}>{t.priority}</td>
                      <td style={td}>{t.attempts}/{t.max_attempts}</td>
                      <td style={td}>{t.leased_by || "—"}</td>
                      <td style={td}>{formatDt(t.lease_expires_at)}</td>
                      <td style={td} onClick={(e) => e.stopPropagation()}>
                        <button
                          onClick={() => cancelTask(t.id)}
                          disabled={isFinal}
                          style={{ padding: "4px 8px" }}
                          title={isFinal ? "Already finished/canceled" : "Cancel task"}
                        >
                          Cancel
                        </button>
                      </td>
                    </tr>

                    {isExpanded && (
                      <tr>
                        <td style={td} colSpan={10}>
                          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
                            <Block title="payload" value={t.payload} />
                            <Block title="result" value={t.result ?? null} />
                          </div>
                          {t.error && (
                            <div style={{ marginTop: 12, padding: 10, border: "1px solid #f99", background: "#fff6f6" }}>
                              <b>error:</b> {t.error}
                            </div>
                          )}
                          <div style={{ marginTop: 8, fontSize: 12, opacity: 0.75 }}>
                            updated_at: {formatDt(t.updated_at)}
                          </div>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                );
              })}

              {!loading && tasks.length === 0 && (
                <tr>
                  <td style={td} colSpan={10}>
                    No tasks found.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      <div style={{ marginTop: 10, fontSize: 12, opacity: 0.8 }}>
        API: {API_BASE}
      </div>
    </div>
  );
}

function Block({ title, value }: { title: string; value: any }) {
  return (
    <div style={{ border: "1px solid #e5e5e5", borderRadius: 8, padding: 10 }}>
      <div style={{ fontSize: 12, fontWeight: 700, marginBottom: 6 }}>{title}</div>
      <pre style={{ margin: 0, fontSize: 12, whiteSpace: "pre-wrap" }}>
        {JSON.stringify(value, null, 2)}
      </pre>
    </div>
  );
}

function StatusPill({ status }: { status: TaskStatus }) {
  const bg =
    status === "queued" ? "#eef" :
    status === "leased" ? "#efe" :
    status === "running" ? "#ffe" :
    status === "done" ? "#eaffea" :
    status === "failed" ? "#ffe6e6" :
    "#f1f1f1";

  return (
    <span style={{
      padding: "2px 8px",
      borderRadius: 999,
      background: bg,
      border: "1px solid #ddd",
      fontSize: 12,
    }}>
      {status}
    </span>
  );
}

const th: React.CSSProperties = { textAlign: "left", padding: 10, borderBottom: "1px solid #ddd", whiteSpace: "nowrap" };
const td: React.CSSProperties = { padding: 10, borderBottom: "1px solid #eee", verticalAlign: "top" };
