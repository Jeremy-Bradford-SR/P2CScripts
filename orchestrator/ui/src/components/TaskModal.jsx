import React, { useState, useEffect } from 'react';
import axios from 'axios';

function TaskModal({ task, jobs, onClose, onSave }) {
    const [name, setName] = useState('');
    const [jobId, setJobId] = useState('');
    const [interval, setInterval] = useState(60);
    const [config, setConfig] = useState('{}');
    const [enabled, setEnabled] = useState(true);

    useEffect(() => {
        if (task) {
            setName(task.name);
            setJobId(task.job_id);
            setInterval(task.interval_minutes);
            setConfig(task.config_json || '{}');
            setEnabled(task.enabled === 1);
        } else {
            // New Task Defaults
            setName('');
            if (jobs.length > 0) setJobId(jobs[0].job_id);
            setInterval(60);
            setConfig('{}');
            setEnabled(true);
        }
    }, [task, jobs]);

    const handleSave = () => {
        let parsedConfig = {};
        try {
            parsedConfig = JSON.parse(config);
        } catch (e) {
            alert("Invalid JSON Config");
            return;
        }

        const payload = {
            job_id: parseInt(jobId),
            name,
            interval_minutes: parseInt(interval),
            config: parsedConfig,
            enabled
        };

        if (task) {
            // Update
            axios.put(`/api/tasks/${task.task_id}`, payload)
                .then(() => onSave())
                .catch(err => alert("Error updating task: " + err));
        } else {
            // Create
            axios.post('/api/tasks', payload)
                .then(() => onSave())
                .catch(err => alert("Error creating task: " + err));
        }
    };

    return (
        <div className="modal-overlay" onClick={onClose}>
            <div className="modal" onClick={e => e.stopPropagation()}>
                <h2>{task ? 'Edit Task' : 'Create Task'}</h2>

                <div className="form-group">
                    <label>Job Script:</label>
                    <select value={jobId} onChange={e => setJobId(e.target.value)} disabled={!!task}>
                        {jobs.map(job => (
                            <option key={job.job_id} value={job.job_id}>{job.name}</option>
                        ))}
                    </select>
                </div>

                <div className="form-group">
                    <label>Task Name:</label>
                    <input
                        type="text"
                        value={name}
                        onChange={e => setName(e.target.value)}
                        placeholder="e.g. Daily Ingestion"
                    />
                </div>

                <div className="form-group">
                    <label>Interval (Minutes):</label>
                    <input
                        type="number"
                        value={interval}
                        onChange={e => setInterval(e.target.value)}
                        min="1"
                    />
                </div>

                <div className="form-group">
                    <label>Configuration (JSON):</label>
                    <textarea
                        className="config-editor"
                        value={config}
                        onChange={e => setConfig(e.target.value)}
                    />
                </div>

                <div className="form-group checkbox-group">
                    <label>
                        <input
                            type="checkbox"
                            checked={enabled}
                            onChange={e => setEnabled(e.target.checked)}
                        />
                        Enabled
                    </label>
                </div>

                <div className="modal-actions">
                    <button className="btn btn-secondary" onClick={onClose}>Cancel</button>
                    <button className="btn" onClick={handleSave}>Save Task</button>
                </div>
            </div>
        </div>
    );
}

export default TaskModal;
