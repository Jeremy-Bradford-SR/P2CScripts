import React, { useState, useEffect, useRef } from 'react';
import axios from 'axios';
import { Play, Search, Terminal, AlertCircle, CheckCircle2, Clock, X, StopCircle } from 'lucide-react';
import { useInterval } from '../hooks/useInterval';

export default function IngestionDashboard() {
    const [jobs, setJobs] = useState([]);
    const [history, setHistory] = useState([]);
    const [logs, setLogs] = useState([]);
    const [selectedRunId, setSelectedRunId] = useState(null);
    const [showRunModal, setShowRunModal] = useState(false);
    const [selectedJob, setSelectedJob] = useState(null);
    const [configJson, setConfigJson] = useState('{}');
    const [isKilling, setIsKilling] = useState({});
    const logsEndRef = useRef(null);

    useEffect(() => { fetchData(); }, []);
    useInterval(() => {
        fetchData();
        if (selectedRunId) fetchLogs(selectedRunId, true);
    }, 2000);

    // Removed aggressive auto-scroll logic so the user can manually review historical chunks.
    const fetchData = async () => {
        try {
            const [j, h] = await Promise.all([axios.get('/api/jobs'), axios.get('/api/history?limit=30')]);
            setJobs(j.data); setHistory(h.data);
        } catch (e) { }
    };

    const fetchLogs = async (runId, isPolling = false) => {
        try {
            const res = await axios.get(`/api/logs/${runId}`);
            setLogs(res.data);
            if (!isPolling) setSelectedRunId(runId);
        } catch (e) { }
    };

    const scanScripts = async () => {
        await axios.post('/api/jobs/scan');
        fetchData();
    };

    const executeJob = async () => {
        if (!selectedJob) return;
        try {
            await axios.post(`/api/jobs/${selectedJob.job_id}/run`, { config: JSON.parse(configJson) });
            setShowRunModal(false);
            fetchData();
        } catch (e) { alert("Execution Error."); }
    };

    const killJob = async (runId) => {
        if (!confirm("Terminate process?")) return;
        setIsKilling(p => ({ ...p, [runId]: true }));
        await axios.post(`/api/jobs/${runId}/kill`);
        fetchData();
        setIsKilling(p => ({ ...p, [runId]: false }));
    };

    const StatusPill = ({ status }) => {
        switch (status) {
            case 'SUCCESS': return <span className="badge badge-success"><CheckCircle2 size={14} /> Success</span>;
            case 'FAILURE': return <span className="badge badge-error"><AlertCircle size={14} /> Error</span>;
            case 'RUNNING': return <span className="badge badge-warning"><Clock size={14} className="animate-pulse" /> Running</span>;
            case 'CANCELLED': return <span className="badge badge-neutral"><StopCircle size={14} /> Halted</span>;
            default: return <span className="badge badge-neutral">{status}</span>;
        }
    };

    return (
        <div style={{ animation: 'fadeIn 0.3s ease' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                <div>
                    <h2 className="page-title">Deployments</h2>
                    <p className="page-desc">Launch background workers and monitor execution pipelines.</p>
                </div>
                <button className="btn btn-secondary" onClick={scanScripts}>
                    <Search size={16} /> Scan Backend
                </button>
            </div>

            <h3 style={{ fontSize: '18px', fontWeight: 600, margin: '24px 0 16px' }}>Available Workers</h3>
            {jobs.length === 0 ? (
                <div style={{ padding: '64px', textAlign: 'center', border: '1px dashed var(--accents-2)', borderRadius: 'var(--radius)', color: 'var(--accents-5)' }}>
                    No executable scripts found.
                </div>
            ) : (
                <div className="card-grid">
                    {jobs.map(job => (
                        <div key={job.job_id} className="v-card">
                            <div className="v-card-header">
                                <div className="v-card-title">{job.name}</div>
                                <div className="v-card-subtitle">{job.script_path.split('/').pop()}</div>
                            </div>
                            <div className="v-card-body"></div>
                            <div className="v-card-footer">
                                <span style={{ fontSize: '13px', color: 'var(--accents-5)' }}>ID: {job.job_id}</span>
                                <button className="btn btn-primary" onClick={() => {
                                    setSelectedJob(job); setConfigJson(job.default_config || '{}'); setShowRunModal(true);
                                }}>
                                    <Play size={14} /> Run
                                </button>
                            </div>
                        </div>
                    ))}
                </div>
            )}

            <h3 style={{ fontSize: '18px', fontWeight: 600, marginBottom: '16px', marginTop: '48px' }}>Pipeline Telemetry</h3>
            <div className="list-container">
                <div className="list-header">
                    <span>Process</span>
                    <span style={{ display: 'none' }}>Actions</span>
                </div>
                {history.length === 0 ? (
                    <div style={{ padding: '32px', textAlign: 'center', color: 'var(--accents-5)' }}>No pipelines executed yet.</div>
                ) : (
                    history.map(run => (
                        <div key={run.run_id} className="list-row">
                            <div className="lr-main">
                                <div className="lr-title">
                                    {run.name}
                                    <StatusPill status={run.status} />
                                </div>
                                <div className="lr-meta">
                                    <span>#{run.run_id}</span>
                                    •
                                    <span>{new Date(run.start_time).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', second: '2-digit' })}</span>
                                </div>
                            </div>
                            <div className="lr-actions">
                                {run.status === 'RUNNING' && (
                                    <button className="btn btn-danger" onClick={() => killJob(run.run_id)} disabled={isKilling[run.run_id]}>
                                        Terminate
                                    </button>
                                )}
                                <button className="btn btn-secondary" onClick={() => fetchLogs(run.run_id)}>
                                    View Logs
                                </button>
                            </div>
                        </div>
                    ))
                )}
            </div>

            {selectedRunId && (
                <div className="slide-over-backdrop" onClick={() => setSelectedRunId(null)}>
                    <div className="slide-over-panel" onClick={e => e.stopPropagation()}>
                        <div className="slide-over-header">
                            <div className="slide-over-title">
                                <Terminal size={20} /> Build Logs {selectedRunId}
                            </div>
                            <button style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--accents-5)' }} onClick={() => setSelectedRunId(null)}>
                                <X size={24} />
                            </button>
                        </div>
                        <div className="slide-over-body">
                            {logs.length === 0 ? (
                                <div style={{ color: '#888' }}>Waiting for output...</div>
                            ) : (
                                logs.map((l, i) => {
                                    let c = '#fff';
                                    if (l.log_text.includes("ERROR")) c = '#ff5555';
                                    if (l.log_text.includes("WARN")) c = '#f1fa8c';
                                    if (l.log_text.includes("SUCCESS")) c = '#50fa7b';
                                    return (
                                        <div key={i} className="log-line">
                                            <span className="log-ts">
                                                {new Date(l.created_at).toISOString().split('T')[1].slice(0, 12)}
                                            </span>
                                            <span style={{ color: c }}>{l.log_text}</span>
                                        </div>
                                    );
                                })
                            )}
                            <div ref={logsEndRef} />
                        </div>
                    </div>
                </div>
            )}

            {showRunModal && selectedJob && (
                <div className="modal-overlay" onClick={() => setShowRunModal(false)}>
                    <div className="modal-content" onClick={e => e.stopPropagation()}>
                        <div className="modal-title">Configure Runtime</div>
                        <div className="input-wrapper">
                            <label className="label">Environment JSON Payload</label>
                            <textarea className="input" value={configJson} onChange={e => setConfigJson(e.target.value)} />
                        </div>
                        <div style={{ display: 'flex', justifyItems: 'flex-end', justifyContent: 'flex-end', gap: '12px', marginTop: '32px' }}>
                            <button className="btn btn-secondary" onClick={() => setShowRunModal(false)}>Cancel</button>
                            <button className="btn btn-primary" onClick={executeJob}>Invoke</button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
