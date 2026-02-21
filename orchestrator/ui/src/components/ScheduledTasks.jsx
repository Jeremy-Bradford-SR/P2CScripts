import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { useInterval } from '../hooks/useInterval';

export default function ScheduledTasks() {
    const [tasks, setTasks] = useState([]);
    const [jobs, setJobs] = useState([]);

    const fetchTasks = async () => {
        try {
            const [tasksRes, jobsRes] = await Promise.all([axios.get('/api/tasks'), axios.get('/api/jobs')]);
            setTasks(tasksRes.data); setJobs(jobsRes.data);
        } catch (err) { }
    };

    useEffect(() => { fetchTasks(); }, []);
    useInterval(fetchTasks, 5000);

    const toggleTask = (taskId, currentState) => {
        axios.patch(`/api/tasks/${taskId}`, { enabled: !currentState }).then(fetchTasks);
    };

    const deleteTask = (taskId) => {
        if (confirm("Delete routine?")) axios.delete(`/api/tasks/${taskId}`).then(fetchTasks);
    };

    return (
        <div style={{ animation: 'fadeIn 0.3s ease' }}>
            <h2 className="page-title">Cron Routines</h2>
            <p className="page-desc" style={{ marginBottom: '48px' }}>Automated chronological triggers mapped to orchestrator execution pipelines.</p>

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
                                <div className="lr-actions" style={{ marginLeft: 'auto' }}>
                                    <button className="btn btn-secondary" onClick={() => toggleTask(task.task_id, task.enabled)}>
                                        {task.enabled ? 'Pause' : 'Resume'}
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
        </div>
    );
}
