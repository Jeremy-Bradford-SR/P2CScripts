import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { useInterval } from '../hooks/useInterval';

export default function ScheduledTasks() {
    const [tasks, setTasks] = useState([]);
    const [jobs, setJobs] = useState([]);

    // Modal State
    const [isModalOpen, setIsModalOpen] = useState(false);
    const [editingTaskId, setEditingTaskId] = useState(null);
    const [formData, setFormData] = useState({
        name: '',
        job_id: '',
        interval_minutes: 60,
        config: '{}',
        enabled: true
    });

    const fetchTasks = async () => {
        try {
            const [tasksRes, jobsRes] = await Promise.all([axios.get('/api/tasks'), axios.get('/api/jobs')]);
            setTasks(tasksRes.data); setJobs(jobsRes.data);
        } catch (err) { }
    };

    useEffect(() => { fetchTasks(); }, []);
    useInterval(fetchTasks, 5000);

    const toggleTask = (taskId, currentState) => {
        axios.put(`/api/tasks/${taskId}`, { enabled: !currentState }).then(fetchTasks);
    };

    const deleteTask = (taskId) => {
        if (confirm("Delete routine?")) axios.delete(`/api/tasks/${taskId}`).then(fetchTasks);
    };

    const openCreateModal = () => {
        setEditingTaskId(null);
        setFormData({ name: '', job_id: jobs[0]?.job_id || '', interval_minutes: 60, config: '{}', enabled: true });
        setIsModalOpen(true);
    };

    const openEditModal = (task) => {
        setEditingTaskId(task.task_id);
        const configStr = typeof task.config_json === 'string' ? task.config_json : JSON.stringify(task.config_json || {});
        setFormData({
            name: task.name,
            job_id: task.job_id,
            interval_minutes: task.interval_minutes,
            config: configStr,
            enabled: task.enabled === 1 || task.enabled === true
        });
        setIsModalOpen(true);
    };

    const saveTask = async () => {
        try {
            const parsedConfig = JSON.parse(formData.config || '{}');
            const payload = {
                name: formData.name,
                job_id: parseInt(formData.job_id),
                interval_minutes: parseInt(formData.interval_minutes),
                config: parsedConfig,
                enabled: formData.enabled
            };

            if (editingTaskId) {
                // job_id cannot be updated via PUT based on our server.py TaskUpdate model
                delete payload.job_id;
                await axios.put(`/api/tasks/${editingTaskId}`, payload);
            } else {
                await axios.post('/api/tasks', payload);
            }

            setIsModalOpen(false);
            fetchTasks();
        } catch (e) {
            alert("Failed to save task. Ensure config is valid JSON.");
        }
    };

    return (
        <div style={{ animation: 'fadeIn 0.3s ease' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '24px' }}>
                <div>
                    <h2 className="page-title">Cron Routines</h2>
                    <p className="page-desc">Automated chronological triggers mapped to orchestrator execution pipelines.</p>
                </div>
                <button className="btn btn-primary" onClick={openCreateModal}>+ Create Routine</button>
            </div>

            <div className="list-container">
                <div className="list-header">
                    <span>Active Triggers</span>
                </div>
                {tasks.length === 0 ? (
                    <div style={{ padding: '64px', textAlign: 'center', color: 'var(--accents-5)' }}>No automated routines defined.</div>
                ) : (
                    tasks.map(task => {
                        const parentJob = jobs.find(j => j.job_id === task.job_id);
                        return (
                            <div key={task.task_id} className="list-row">
                                <div className="lr-main">
                                    <div className="lr-title">
                                        {task.name}
                                        <span className={`badge ${task.enabled ? 'badge-success' : 'badge-neutral'}`}>
                                            {task.enabled ? 'Active' : 'Paused'}
                                        </span>
                                    </div>
                                    <div className="lr-meta">
                                        <span>Target: {parentJob ? parentJob.name : 'Unknown'}</span>
                                        •
                                        <span>Interval: {task.interval_minutes}m</span>
                                        •
                                        <span>Next: {task.next_run ? new Date(task.next_run).toLocaleString(undefined, { hour: 'numeric', minute: '2-digit' }) : 'Pending'}</span>
                                    </div>
                                </div>
                                <div className="lr-actions" style={{ marginLeft: 'auto', display: 'flex', gap: '8px' }}>
                                    <button className="btn btn-secondary" onClick={() => toggleTask(task.task_id, task.enabled)}>
                                        {task.enabled ? 'Pause' : 'Resume'}
                                    </button>
                                    <button className="btn btn-secondary" onClick={() => openEditModal(task)}>
                                        Edit
                                    </button>
                                    <button className="btn btn-danger" onClick={() => deleteTask(task.task_id)}>
                                        Delete
                                    </button>
                                </div>
                            </div>
                        );
                    })
                )}
            </div>

            {isModalOpen && (
                <div className="modal-overlay" onClick={() => setIsModalOpen(false)}>
                    <div className="modal-content" onClick={e => e.stopPropagation()}>
                        <h3 style={{ marginTop: 0 }}>{editingTaskId ? 'Edit Routine' : 'Create Routine'}</h3>

                        <div style={{ marginBottom: '16px' }}>
                            <label className="filter-label">Target Script</label>
                            <select
                                className="filter-select"
                                value={formData.job_id}
                                onChange={e => setFormData({ ...formData, job_id: e.target.value })}
                                disabled={editingTaskId !== null} // Cannot change script once created
                                style={{ width: '100%', padding: '8px', background: 'var(--background)', color: 'var(--foreground)', border: '1px solid var(--accents-2)' }}
                            >
                                <option value="" disabled>Select a target...</option>
                                {jobs.map(j => <option key={j.job_id} value={j.job_id}>{j.name}</option>)}
                            </select>
                        </div>

                        <div style={{ marginBottom: '16px' }}>
                            <label className="filter-label">Routine Name</label>
                            <input
                                type="text"
                                value={formData.name}
                                onChange={e => setFormData({ ...formData, name: e.target.value })}
                                style={{ width: '100%', padding: '8px', background: 'var(--background)', color: 'var(--foreground)', border: '1px solid var(--accents-2)' }}
                            />
                        </div>

                        <div style={{ marginBottom: '16px' }}>
                            <label className="filter-label">Interval (Minutes)</label>
                            <input
                                type="number"
                                value={formData.interval_minutes}
                                onChange={e => setFormData({ ...formData, interval_minutes: e.target.value })}
                                style={{ width: '100%', padding: '8px', background: 'var(--background)', color: 'var(--foreground)', border: '1px solid var(--accents-2)' }}
                            />
                        </div>

                        <div style={{ marginBottom: '16px' }}>
                            <label className="filter-label">JSON Config Overrides (Optional)</label>
                            <textarea
                                value={formData.config}
                                onChange={e => setFormData({ ...formData, config: e.target.value })}
                                rows={4}
                                style={{ width: '100%', padding: '8px', background: 'var(--accents-1)', color: 'var(--foreground)', border: '1px solid var(--accents-2)', fontFamily: 'monospace' }}
                            />
                        </div>

                        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '8px', marginTop: '24px' }}>
                            <button className="btn btn-secondary" onClick={() => setIsModalOpen(false)}>Cancel</button>
                            <button className="btn btn-primary" onClick={saveTask}>Save Routine</button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
