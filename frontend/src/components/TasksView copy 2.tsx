import "../styles/tasks.css";

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

const API_BASE =
  (import.meta as any).env?.VITE_TASK_API_URL?.replace(/\/$/, "") || "http://127.0.0.1:8000";

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
    status === "queued"
      ? "#eef"
      : status === "leased"
        ? "#efe"
        : status === "running"
          ? "#ffe"
          : status === "done"
            ? "#eaffea"
            : status === "failed"
              ? "#ffe6e6"
              : "#f1f1f1";

  return (
    <span
      style={{
        padding: "2px 8px",
        borderRadius: 999,
        background: bg,
        border: "1px solid #ddd",
        fontSize: 12,
        fontWeight: 600,
      }}
    >
      {status}
    </span>
  );
}

const TASK_TYPES = [
  "latin_square_from_prefix",
  "mols_search",
  "front_test_latin_square_from_prefix",
  "front_test_mols_search",
] as const;

type TaskType = (typeof TASK_TYPES)[number];

export default function TasksView() {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  // filters
  const [status, setStatus] = useState<TaskStatus | "">("");
  const [taskType, setTaskType] = useState<TaskType | "">("");
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
    <div className="tasks-page">
      <h2 className="tasks-title">Tasks</h2>

      {/* Filters */}
      <div className="filters">
        <div className="filters-grid">
          {/* Левая группа: фильтры */}
          <div className="filters-left">
            <div className="field">
              <label>Status</label>
              <select className="select" value={status} onChange={(e) => setStatus(e.target.value as any)}>
                <option value="">All</option>
                <option value="queued">queued</option>
                <option value="leased">leased</option>
                <option value="running">running</option>
                <option value="done">done</option>
                <option value="failed">failed</option>
                <option value="canceled">canceled</option>
              </select>
            </div>

            <div className="field">
              <label>Task type</label>
              <select
                className="select"
                value={taskType}
                onChange={(e) => setTaskType(e.target.value as any)}
              >
                <option value="">All</option>
                {TASK_TYPES.map((t) => (
                  <option key={t} value={t}>
                    {t}
                  </option>
                ))}
              </select>
            </div>

            <div className="field">
              <label>n</label>
              <input className="input" value={n} onChange={(e) => setN(e.target.value)} placeholder="5" />
            </div>
          </div>

          {/* Правая группа: order/limit/actions */}
          <div className="filters-right">
            <div className="field">
              <label>Order</label>
              <select className="select" value={order} onChange={(e) => setOrder(e.target.value as any)}>
                <option value="created_at_desc">created_at ↓</option>
                <option value="created_at_asc">created_at ↑</option>
                <option value="priority_desc">priority ↓</option>
              </select>
            </div>

            <div className="field">
              <label>Limit</label>
              <input
                className="input"
                type="number"
                value={limit}
                min={1}
                max={500}
                onChange={(e) => setLimit(Number(e.target.value))}
              />
            </div>

            <div className="actions">
              <button className="btn btn-primary" onClick={() => load()} disabled={loading}>
                {loading ? "Loading…" : "Refresh"}
              </button>
              <label className="chk">
                <input type="checkbox" checked={autoRefresh} onChange={(e) => setAutoRefresh(e.target.checked)} />
                auto
              </label>
            </div>
          </div>
        </div>
      </div>


      {/* Paging */}
      <div className="paging">
        <button className="btn" onClick={() => setOffset((x) => Math.max(0, x - limit))} disabled={offset === 0}>
          ← Prev
        </button>
        <button className="btn" onClick={() => setOffset((x) => x + limit)} disabled={tasks.length < limit}>
          Next →
        </button>
        <span className="pill">offset: {offset}</span>
      </div>

      {err && (
        <div className="err">
          <b>Error:</b> {err}
          <div style={{ fontSize: 12, opacity: 0.8 }}>API: {API_BASE}</div>
        </div>
      )}

      {/* Table */}
      <div className="table-wrap">
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Created</th>
                <th>ID</th>
                <th>Type</th>
                <th>Status</th>
                <th>n</th>
                <th>Priority</th>
                <th>Attempts</th>
                <th>Leased by</th>
                <th>Lease expires</th>
                <th>Actions</th>
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
                      style={{ cursor: "pointer", background: isExpanded ? "#fafafa" : "white" }}
                    >
                      <td>{formatDt(t.created_at)}</td>
                      <td title={t.id}>{shortId(t.id)}</td>
                      <td>{t.task_type}</td>
                      <td>
                        <StatusPill status={t.status} />
                      </td>
                      <td>{t.n}</td>
                      <td>{t.priority}</td>
                      <td>
                        {t.attempts}/{t.max_attempts}
                      </td>
                      <td>{t.leased_by || "—"}</td>
                      <td>{formatDt(t.lease_expires_at)}</td>

                      <td className="row-actions" onClick={(e) => e.stopPropagation()}>
                        <button
                          className="btn"
                          onClick={() => cancelTask(t.id)}
                          disabled={isFinal}
                          title={isFinal ? "Already finished/canceled" : "Cancel task"}
                        >
                          Cancel
                        </button>
                      </td>
                    </tr>

                    {isExpanded && (
                      <tr>
                        <td colSpan={10}>
                          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
                            <Block title="payload" value={t.payload} />
                            <Block title="result" value={t.result ?? null} />
                          </div>

                          {t.error && (
                            <div
                              style={{
                                marginTop: 12,
                                padding: 10,
                                border: "1px solid #f99",
                                background: "#fff6f6",
                                borderRadius: 10,
                              }}
                            >
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
                  <td colSpan={10}>No tasks found.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      <div style={{ marginTop: 10, fontSize: 12, opacity: 0.7 }}>API: {API_BASE}</div>
    </div>
  );
}
