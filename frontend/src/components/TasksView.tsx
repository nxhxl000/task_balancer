import "../styles/tasks.css";

import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";

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

  payload: Record<string, unknown> | null;
  result?: Record<string, unknown> | null;
  error?: string | null;

  created_at: string;
  updated_at: string;
};

type Order = "created_at_desc" | "created_at_asc" | "priority_desc";

const API_BASE =
  (import.meta as any).env?.VITE_TASK_API_URL?.replace(/\/$/, "") || "http://127.0.0.1:8000";

const TASK_TYPES = [
  "latin_square_from_prefix",
  "mols_search",
  "front_test_latin_square_from_prefix",
  "front_test_mols_search",
] as const;

type TaskType = (typeof TASK_TYPES)[number];

function shortId(id: string) {
  return id.length > 12 ? `${id.slice(0, 8)}…${id.slice(-4)}` : id;
}

function formatDt(s?: string | null) {
  if (!s) return "—";
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return s;
  return d.toLocaleString();
}

function isFinalStatus(s: TaskStatus) {
  return s === "done" || s === "failed" || s === "canceled";
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

function payloadSummary(payload: Record<string, unknown> | null): string {
  const p: any = payload ?? {};
  const parts: string[] = [];

  if (typeof p.problem === "string") parts.push(p.problem);
  if (typeof p.method === "string") parts.push(p.method);

  const keys = Object.keys(p).filter((k) => k !== "problem" && k !== "method");
  if (keys.length) parts.push(...keys.slice(0, 2));

  let s = parts.join(" • ");
  if (!s) s = "{…}";
  if (keys.length > 2) s += " …";
  return s;
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
        fontWeight: 700,
      }}
    >
      {status}
    </span>
  );
}

type CreateMode = "latin" | "mols" | null;

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
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [selectedId, setSelectedId] = useState<string | null>(null);

  // fixed horizontal scrollbar sync
  const tableScrollRef = useRef<HTMLDivElement | null>(null);
  const hscrollRef = useRef<HTMLDivElement | null>(null);
  const hscrollInnerRef = useRef<HTMLDivElement | null>(null);
  const [showHScroll, setShowHScroll] = useState(false);
  const syncing = useRef(false);

  // create task UI
  const [createMode, setCreateMode] = useState<CreateMode>(null);
  const [createBusy, setCreateBusy] = useState(false);
  const [createErr, setCreateErr] = useState<string | null>(null);

  // --- Create Latin defaults/fields ---
  const [latinTaskN, setLatinTaskN] = useState<number>(5);
  const [latinPriority, setLatinPriority] = useState<number>(50);
  const [latinMaxAttempts, setLatinMaxAttempts] = useState<number>(10);

  const [latinReturnOne, setLatinReturnOne] = useState<boolean>(true);
  const [latinFixFirstRow, setLatinFixFirstRow] = useState<boolean>(true);

  const [latinPrefixText, setLatinPrefixText] = useState<string>(
    JSON.stringify(
      [
        [0, 1, 2, 3, 4],
        [null, null, null, null, null],
        [null, null, null, null, null],
        [null, null, null, null, null],
        [null, null, null, null, null],
      ],
      null,
      2
    )
  );

  // --- Create MOLS defaults/fields ---
  const [molsTaskN, setMolsTaskN] = useState<number>(5);
  const [molsPriority, setMolsPriority] = useState<number>(50);
  const [molsMaxAttempts, setMolsMaxAttempts] = useState<number>(10);

  const [molsK, setMolsK] = useState<number>(2);
  const [molsPayloadN, setMolsPayloadN] = useState<number>(9);
  const [molsSeed, setMolsSeed] = useState<number>(45203843);
  const [molsBudgetMaxSteps, setMolsBudgetMaxSteps] = useState<number>(2000000);
  const [molsTimeLimit, setMolsTimeLimit] = useState<number>(600);
  const [molsMethod, setMolsMethod] = useState<string>("Jacobson-Matthews");

  const selectedTask = useMemo(
    () => (selectedId ? tasks.find((t) => t.id === selectedId) ?? null : null),
    [tasks, selectedId]
  );

  const selectedIsFinal = selectedTask ? isFinalStatus(selectedTask.status) : true;

  const query = useMemo(() => {
    const p = new URLSearchParams();
    if (status) p.set("status", status);
    if (taskType) p.set("task_type", taskType);
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

      if (selectedId && !data.some((t) => t.id === selectedId)) {
        setSelectedId(null);
      }
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

  async function createTask() {
    if (!createMode) return;

    setCreateBusy(true);
    setCreateErr(null);

    try {
      let newTask: any;

      if (createMode === "latin") {
        let prefixParsed: any = null;
        try {
          prefixParsed = JSON.parse(latinPrefixText);
        } catch {
          throw new Error("Latin: prefix должен быть валидным JSON (матрица).");
        }

        newTask = {
          task_type: "latin_square_from_prefix",
          n: Number(latinTaskN),
          priority: Number(latinPriority),
          max_attempts: Number(latinMaxAttempts),
          payload: {
            problem: "complete_latin_square_from_prefix",
            output: { return_one_solution: Boolean(latinReturnOne) },
            prefix_format: "matrix_nulls",
            prefix: prefixParsed,
            constraints: {
              latin: true,
              symmetry_breaking: { fix_first_row: Boolean(latinFixFirstRow) },
            },
          },
        };
      }

      if (createMode === "mols") {
        newTask = {
          task_type: "mols_search",
          n: Number(molsTaskN),
          priority: Number(molsPriority),
          max_attempts: Number(molsMaxAttempts),
          payload: {
            k: Number(molsK),
            n: Number(molsPayloadN),
            seed: Number(molsSeed),
            budget: {
              max_steps: Number(molsBudgetMaxSteps),
              time_limit_sec: Number(molsTimeLimit),
            },
            method: String(molsMethod || "Jacobson-Matthews"),
            problem: "search_mols",
          },
        };
      }

      const resp = await api<any>(`/tasks`, {
        method: "POST",
        body: JSON.stringify(newTask),
      });

      // если API вернул созданную таску (или {id: ...})
      const createdId = resp?.id ?? resp?.task?.id ?? null;

      setCreateMode(null);
      await load();
      if (createdId) setSelectedId(createdId);
    } catch (e: any) {
      setCreateErr(e?.message || String(e));
    } finally {
      setCreateBusy(false);
    }
  }

  // initial + on query change
  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [query]);

  // auto refresh
  useEffect(() => {
    if (!autoRefresh) return;
    const t = setInterval(() => load(), 3000);
    return () => clearInterval(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoRefresh, query]);

  // --- fixed horizontal scrollbar helpers ---
  function syncScrollbarSize() {
    const el = tableScrollRef.current;
    const inner = hscrollInnerRef.current;
    const bar = hscrollRef.current;
    if (!el || !inner || !bar) return;

    inner.style.width = `${el.scrollWidth}px`;

    const need = el.scrollWidth > el.clientWidth + 1;
    setShowHScroll(need);

    bar.scrollLeft = el.scrollLeft;
  }

  useLayoutEffect(() => {
    const el = tableScrollRef.current;
    if (!el) return;

    const raf = requestAnimationFrame(() => syncScrollbarSize());

    const onResize = () => syncScrollbarSize();
    window.addEventListener("resize", onResize);

    const ro = new ResizeObserver(() => syncScrollbarSize());
    ro.observe(el);

    return () => {
      cancelAnimationFrame(raf);
      window.removeEventListener("resize", onResize);
      ro.disconnect();
    };
  }, [tasks, selectedId]);

  useEffect(() => {
    const el = tableScrollRef.current;
    const bar = hscrollRef.current;
    if (!el || !bar) return;

    const onElScroll = () => {
      if (syncing.current) return;
      syncing.current = true;
      bar.scrollLeft = el.scrollLeft;
      requestAnimationFrame(() => (syncing.current = false));
    };

    const onBarScroll = () => {
      if (syncing.current) return;
      syncing.current = true;
      el.scrollLeft = bar.scrollLeft;
      requestAnimationFrame(() => (syncing.current = false));
    };

    el.addEventListener("scroll", onElScroll, { passive: true });
    bar.addEventListener("scroll", onBarScroll, { passive: true });

    return () => {
      el.removeEventListener("scroll", onElScroll as any);
      bar.removeEventListener("scroll", onBarScroll as any);
    };
  }, []);

  return (
    <div className="tasks-page">
      <h2 className="tasks-title">Tasks</h2>

      {/* Filters */}
      <div className="filters">
        <div className="filters-grid">
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
              <select className="select" value={taskType} onChange={(e) => setTaskType(e.target.value as any)}>
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

      {/* Table + Right column */}
      <div className="tasks-main">
        {/* Table */}
        <div className="table-wrap">
          <div className="table-scroll" ref={tableScrollRef}>
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
                  <th>Payload</th>
                  <th>Leased by</th>
                  <th>Lease expires</th>
                  <th>Actions</th>
                </tr>
              </thead>

              <tbody>
                {tasks.map((t) => {
                  const rowFinal = isFinalStatus(t.status);
                  const isSelected = selectedId === t.id;

                  return (
                    <tr
                      key={t.id}
                      className={isSelected ? "is-selected" : ""}
                      onClick={() => setSelectedId(t.id)}
                      style={{ cursor: "pointer" }}
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

                      <td
                        onClick={(e) => {
                          e.stopPropagation();
                          setSelectedId(t.id);
                        }}
                        title="Click to view payload"
                      >
                        <span className="payload-short">{payloadSummary(t.payload)}</span>
                      </td>

                      <td>{t.leased_by || "—"}</td>
                      <td>{formatDt(t.lease_expires_at)}</td>

                      <td className="row-actions" onClick={(e) => e.stopPropagation()}>
                        <button className="btn" onClick={() => cancelTask(t.id)} disabled={rowFinal}>
                          Cancel
                        </button>
                      </td>
                    </tr>
                  );
                })}

                {!loading && tasks.length === 0 && (
                  <tr>
                    <td colSpan={11}>No tasks found.</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

        {/* Right column: Create + Details (one under another) */}
        <div className="side-stack">
          {/* CREATE PANEL */}
          <section className="panel">
            <div className="panel-head">
              <div className="panel-title">Create task</div>
            </div>

            <div className="panel-body">
              <div className="create-choices">
                <button
                  className={`btn ${createMode === "latin" ? "btn-primary" : ""}`}
                  onClick={() => {
                    setCreateErr(null);
                    setCreateMode("latin");
                  }}
                >
                  Create Latin
                </button>

                <button
                  className={`btn ${createMode === "mols" ? "btn-primary" : ""}`}
                  onClick={() => {
                    setCreateErr(null);
                    setCreateMode("mols");
                  }}
                >
                  Create MOLS
                </button>
              </div>

              {createMode && (
                <>
                  <div className="create-subtitle">
                    {createMode === "latin" ? "latin_square_from_prefix" : "mols_search"}
                  </div>

                  {createErr && <div className="form-err">{createErr}</div>}

                  {/* FORM (single column) */}
                  <div className="create-form">
                    {createMode === "latin" && (
                      <>
                        <div className="field">
                          <label>n (task)</label>
                          <input
                            className="input"
                            type="number"
                            value={latinTaskN}
                            onChange={(e) => setLatinTaskN(Number(e.target.value))}
                          />
                        </div>

                        <div className="field">
                          <label>priority</label>
                          <input
                            className="input"
                            type="number"
                            value={latinPriority}
                            onChange={(e) => setLatinPriority(Number(e.target.value))}
                          />
                        </div>

                        <div className="field">
                          <label>max_attempts</label>
                          <input
                            className="input"
                            type="number"
                            value={latinMaxAttempts}
                            onChange={(e) => setLatinMaxAttempts(Number(e.target.value))}
                          />
                        </div>

                        <div className="field field-inline">
                          <label>
                            <input
                              type="checkbox"
                              checked={latinReturnOne}
                              onChange={(e) => setLatinReturnOne(e.target.checked)}
                            />
                            return_one_solution
                          </label>
                        </div>

                        <div className="field field-inline">
                          <label>
                            <input
                              type="checkbox"
                              checked={latinFixFirstRow}
                              onChange={(e) => setLatinFixFirstRow(e.target.checked)}
                            />
                            symmetry_breaking.fix_first_row
                          </label>
                        </div>

                        <div className="field">
                          <label>prefix (JSON matrix)</label>
                          <textarea
                            className="textarea"
                            value={latinPrefixText}
                            onChange={(e) => setLatinPrefixText(e.target.value)}
                            rows={10}
                          />
                        </div>
                      </>
                    )}

                    {createMode === "mols" && (
                      <>
                        <div className="field">
                          <label>n (task)</label>
                          <input
                            className="input"
                            type="number"
                            value={molsTaskN}
                            onChange={(e) => setMolsTaskN(Number(e.target.value))}
                          />
                        </div>

                        <div className="field">
                          <label>priority</label>
                          <input
                            className="input"
                            type="number"
                            value={molsPriority}
                            onChange={(e) => setMolsPriority(Number(e.target.value))}
                          />
                        </div>

                        <div className="field">
                          <label>max_attempts</label>
                          <input
                            className="input"
                            type="number"
                            value={molsMaxAttempts}
                            onChange={(e) => setMolsMaxAttempts(Number(e.target.value))}
                          />
                        </div>

                        <div className="field">
                          <label>k</label>
                          <input
                            className="input"
                            type="number"
                            value={molsK}
                            onChange={(e) => setMolsK(Number(e.target.value))}
                          />
                        </div>

                        <div className="field">
                          <label>payload.n</label>
                          <input
                            className="input"
                            type="number"
                            value={molsPayloadN}
                            onChange={(e) => setMolsPayloadN(Number(e.target.value))}
                          />
                        </div>

                        <div className="field">
                          <label>seed</label>
                          <input
                            className="input"
                            type="number"
                            value={molsSeed}
                            onChange={(e) => setMolsSeed(Number(e.target.value))}
                          />
                        </div>

                        <div className="field">
                          <label>budget.max_steps</label>
                          <input
                            className="input"
                            type="number"
                            value={molsBudgetMaxSteps}
                            onChange={(e) => setMolsBudgetMaxSteps(Number(e.target.value))}
                          />
                        </div>

                        <div className="field">
                          <label>budget.time_limit_sec</label>
                          <input
                            className="input"
                            type="number"
                            value={molsTimeLimit}
                            onChange={(e) => setMolsTimeLimit(Number(e.target.value))}
                          />
                        </div>

                        <div className="field">
                          <label>method</label>
                          <input className="input" value={molsMethod} onChange={(e) => setMolsMethod(e.target.value)} />
                        </div>
                      </>
                    )}

                    <div className="create-actions">
                      <button className="btn btn-primary" onClick={createTask} disabled={createBusy}>
                        {createBusy ? "Creating…" : "Create"}
                      </button>
                      <button className="btn" onClick={() => setCreateMode(null)} disabled={createBusy}>
                        Close
                      </button>
                    </div>
                  </div>
                </>
              )}
            </div>
          </section>

          {/* DETAILS PANEL */}
          <aside className="details">
            <div className="details-head">
              <div className="details-title">Task details</div>

              <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
                <button
                  className="btn"
                  style={{ height: 32 }}
                  onClick={() => selectedTask && cancelTask(selectedTask.id)}
                  disabled={!selectedTask || selectedIsFinal}
                  title={!selectedTask ? "Select a task" : selectedIsFinal ? "Already finished/canceled" : "Cancel task"}
                >
                  Cancel
                </button>

                <button className="btn" style={{ height: 32 }} onClick={() => setSelectedId(null)} disabled={!selectedTask}>
                  Close
                </button>
              </div>
            </div>

            <div className="details-body">
              {!selectedTask ? (
                <div style={{ color: "#64748b", fontSize: 13 }}>Click a row (or payload) to view details.</div>
              ) : (
                <>
                  <div className="kv">
                    <span>ID</span>
                    <span title={selectedTask.id}>{selectedTask.id}</span>

                    <span>task_type</span>
                    <span>{selectedTask.task_type}</span>

                    <span>status</span>
                    <span>
                      <StatusPill status={selectedTask.status} />
                    </span>

                    <span>n</span>
                    <span>{selectedTask.n}</span>

                    <span>priority</span>
                    <span>{selectedTask.priority}</span>

                    <span>attempts</span>
                    <span>
                      {selectedTask.attempts}/{selectedTask.max_attempts}
                    </span>

                    <span>max_attempts</span>
                    <span>{selectedTask.max_attempts}</span>

                    <span>leased_by</span>
                    <span>{selectedTask.leased_by ?? "—"}</span>

                    <span>lease_expires_at</span>
                    <span>{formatDt(selectedTask.lease_expires_at)}</span>

                    <span>created_at</span>
                    <span>{formatDt(selectedTask.created_at)}</span>

                    <span>updated_at</span>
                    <span>{formatDt(selectedTask.updated_at)}</span>

                    <span>error</span>
                    <span>{selectedTask.error ?? "—"}</span>
                  </div>

                  <div style={{ fontSize: 12, fontWeight: 900, margin: "10px 0 6px" }}>payload</div>
                  <pre className="jsonbox">{JSON.stringify(selectedTask.payload ?? null, null, 2)}</pre>

                  <div style={{ fontSize: 12, fontWeight: 900, margin: "10px 0 6px" }}>result</div>
                  <pre className="jsonbox">{JSON.stringify(selectedTask.result ?? null, null, 2)}</pre>

                  {selectedTask.error && (
                    <>
                      <div style={{ fontSize: 12, fontWeight: 900, margin: "10px 0 6px" }}>error</div>
                      <div className="jsonbox">{selectedTask.error}</div>
                    </>
                  )}
                </>
              )}
            </div>
          </aside>
        </div>
      </div>

      <div style={{ marginTop: 10, fontSize: 12, opacity: 0.7 }}>API: {API_BASE}</div>

      {/* fixed bottom horizontal scrollbar */}
      <div className={`hscroll ${showHScroll ? "" : "hidden"}`} ref={hscrollRef}>
        <div className="hscroll-inner" ref={hscrollInnerRef} />
      </div>
    </div>
  );
}
