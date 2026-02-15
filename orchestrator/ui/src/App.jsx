import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './App.css';

import ProxyStatus from './components/ProxyStatus';

import ProxyTab from './components/ProxyTab';
import TaskTab from './components/TaskTab';

function App() {
    const [activeTab, setActiveTab] = useState('ingestion');

    // Ingestion State
    const [jobs, setJobs] = useState([]);
    const [history, setHistory] = useState([]);
    const [logs, setLogs] = useState([]);
    const [selectedRunId, setSelectedRunId] = useState(null);
    const [running, setRunning] = useState({});

    // Modal State
    const [showRunModal, setShowRunModal] = useState(false);
    const [selectedJob, setSelectedJob] = useState(null);
    const [configJson, setConfigJson] = useState('{}');

    const fetchJobs = () => {
        axios.get('/api/jobs').then(res => setJobs(res.data));
    };

    const fetchHistory = () => {
        axios.get('/api/history').then(res => setHistory(res.data));
    };

    const fetchLogs = (runId) => {
        axios.get(`/api/logs/${runId}`).then(res => {
            setLogs(res.data);
            setSelectedRunId(runId);
        }).catch(err => alert("Failed to fetch logs: " + err));
    };

    const scanScripts = () => {
        if (confirm("Scan directory for new scripts?")) {
            axios.post('/api/jobs/scan')
                .then(() => {
                    alert("Scan complete.");
                    fetchJobs();
                })
                .catch(err => alert("Scan failed: " + err));
        }
    };

    const openRunModal = (job) => {
        setSelectedJob(job);
        setConfigJson(job.default_config || '{}');
        setShowRunModal(true);
    };

    const executeJob = () => {
        if (!selectedJob) return;

        let config = {};
        try {
            config = JSON.parse(configJson);
        } catch (e) {
            alert("Invalid JSON Config");
            return;
        }

        const jobId = selectedJob.job_id;
        setRunning(prev => ({ ...prev, [jobId]: true }));
        setShowRunModal(false);

        axios.post(`/api/jobs/${jobId}/run`, { config })
            .then(() => {
                setTimeout(fetchHistory, 1000);
            })
            .catch(err => alert("Error starting job: " + err))
            .finally(() => setRunning(prev => ({ ...prev, [jobId]: false })));
    };

    useEffect(() => {
        fetchJobs();
        fetchHistory();
        const interval = setInterval(fetchHistory, 5000);
        return () => clearInterval(interval);
    }, []);

    return (
        <div className="container">
            <header style={{ textAlign: 'center', marginBottom: '2rem' }}>
                <h1 style={{ fontSize: '2.5rem', marginBottom: '0.5rem' }}>P2C Orchestrator</h1>
                <p style={{ color: '#64748b' }}>Centralized Script Management & Proxy Network</p>
            </header>

            {/* Tab Navigation */}
            <div className="tab-nav">
                <button
                    className={`tab-btn ${activeTab === 'ingestion' ? 'active' : ''}`}
                    onClick={() => setActiveTab('ingestion')}
                >
                    📥 Ingestion Engine
                </button>
                <button
                    className={`tab-btn ${activeTab === 'proxies' ? 'active' : ''}`}
                    onClick={() => setActiveTab('proxies')}
                >
                    🌐 Proxy Network
                </button>
                <button
                    className={`tab-btn ${activeTab === 'tasks' ? 'active' : ''}`}
                    onClick={() => setActiveTab('tasks')}
                >
                    ⏱️ Tasks
                </button>
            </div>

            {/* Ingestion Tab */}
            {activeTab === 'ingestion' && (
                <div className="animate-fade-in">
                    <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '10px', marginBottom: '1rem' }}>
                        <button className="btn btn-secondary" onClick={scanScripts}>Scan Scripts</button>
                        <button className="btn btn-secondary" onClick={fetchJobs}>Refresh Data</button>
                    </div>

                    <section className="card mb-6">
                        <h2>Available Jobs</h2>
                        <div className="job-list">
                            {jobs.map(job => (
                                <div key={job.job_id} className="job-card">
                                    <div>
                                        <h3>{job.name}</h3>
                                        <p>{job.script_path}</p>
                                    </div>
                                    <div className="job-actions">
                                        <button
                                            className="btn"
                                            onClick={() => openRunModal(job)}
                                            disabled={running[job.job_id]}
                                        >
                                            {running[job.job_id] ? 'Running...' : 'Run...'}
                                        </button>
                                    </div>
                                </div>
                            ))}
                            {jobs.length === 0 && <p style={{ color: '#888' }}>No jobs found. Click "Scan Scripts" to discover Python files.</p>}
                        </div>
                    </section>

                    <div style={{ display: 'flex', gap: '2rem', flexDirection: 'column' }}>
                        <section className="card">
                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                <h2>Execution History</h2>
                                <button className="btn btn-secondary" onClick={fetchHistory} style={{ fontSize: '0.8rem' }}>Refresh</button>
                            </div>
                            <table className="w-full">
                                <thead>
                                    <tr>
                                        <th>#</th>
                                        <th>Job</th>
                                        <th>Start Time</th>
                                        <th>Status</th>
                                        <th>Actions</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {history.map(run => (
                                        <tr key={run.run_id}>
                                            <td>{run.run_id}</td>
                                            <td>{run.name}</td>
                                            <td>{new Date(run.start_time).toLocaleString()}</td>
                                            <td><span className={`status-${run.status}`}>{run.status}</span></td>
                                            <td>
                                                <button className="btn btn-secondary" style={{ padding: '4px 8px', fontSize: '0.8rem' }} onClick={() => fetchLogs(run.run_id)}>
                                                    View Logs
                                                </button>
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </section>

                        {selectedRunId && (
                            <section className="card">
                                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '1rem' }}>
                                    <h2>Logs for Run #{selectedRunId}</h2>
                                    <button className="btn btn-secondary" onClick={() => setSelectedRunId(null)}>Close Logs</button>
                                </div>
                                <div className="logs-box">
                                    {logs.length === 0 ? <div style={{ padding: '1rem' }}>No logs found/recorded.</div> : logs.map((l, i) => (
                                        <div key={i} className="log-entry">
                                            <span className="log-ts">[{new Date(l.created_at).toLocaleTimeString()}]</span>
                                            {l.log_text}
                                        </div>
                                    ))}
                                </div>
                            </section>
                        )}
                    </div>
                </div>
            )}

            {/* Proxy Tab */}
            {activeTab === 'proxies' && (
                <ProxyTab />
            )}

            {/* Tasks Tab */}
            {activeTab === 'tasks' && (
                <TaskTab />
            )}

            {/* Run Modal (Global) */}
            {showRunModal && selectedJob && (
                <div className="modal-overlay" onClick={() => setShowRunModal(false)}>
                    <div className="modal" onClick={e => e.stopPropagation()}>
                        <h2>Run Job: {selectedJob.name}</h2>
                        <p style={{ color: '#aaa', marginBottom: '1rem' }}>Configure parameters (JSON) before running.</p>

                        <textarea
                            className="config-editor"
                            value={configJson}
                            onChange={e => setConfigJson(e.target.value)}
                        />

                        <div className="modal-actions">
                            <button className="btn btn-secondary" onClick={() => setShowRunModal(false)}>Cancel</button>
                            <button className="btn" onClick={executeJob}>Start Job</button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}

export default App;
