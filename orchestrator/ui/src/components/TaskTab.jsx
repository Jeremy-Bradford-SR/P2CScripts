import React, { useState, useEffect } from 'react';
import axios from 'axios';
import TaskModal from './TaskModal';

function TaskTab() {
    const [tasks, setTasks] = useState([]);
    const [jobs, setJobs] = useState([]);
    const [showModal, setShowModal] = useState(false);
    const [editingTask, setEditingTask] = useState(null);

    const fetchTasks = () => {
        axios.get('/api/tasks').then(res => setTasks(res.data));
    };

    const fetchJobs = () => {
        axios.get('/api/jobs').then(res => setJobs(res.data));
    };

    useEffect(() => {
        fetchTasks();
        fetchJobs();
        const interval = setInterval(fetchTasks, 5000);
        return () => clearInterval(interval);
    }, []);

    const handleCreate = () => {
        setEditingTask(null);
        setShowModal(true);
    };

    const handleEdit = (task) => {
        setEditingTask(task);
        setShowModal(true);
    };

    const handleDelete = (taskId) => {
        if (confirm("Are you sure you want to delete this task?")) {
            axios.delete(`/api/tasks/${taskId}`)
                .then(() => fetchTasks())
                .catch(err => alert("Error deleting task: " + err));
        }
    };

    const handleRunNow = (taskId) => {
        axios.post(`/api/tasks/${taskId}/run`)
            .then(() => {
                alert("Task scheduled for immediate execution.");
                fetchTasks();
            })
            .catch(err => alert("Error running task: " + err));
    };

    const handleSave = () => {
        setShowModal(false);
        fetchTasks();
    };

    return (
        <div className="animate-fade-in">
            <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: '1rem' }}>
                <button className="btn btn-primary" onClick={handleCreate}>+ Create Task</button>
            </div>

            <section className="card">
                <h2>Scheduled Tasks</h2>
                <table className="w-full">
                    <thead>
                        <tr>
                            <th>Name</th>
                            <th>Job</th>
                            <th>Interval</th>
                            <th>Last Run</th>
                            <th>Next Run</th>
                            <th>Status</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody>
                        {tasks.map(task => (
                            <tr key={task.task_id}>
                                <td>{task.name}</td>
                                <td>{task.job_name}</td>
                                <td>{task.interval_minutes}m</td>
                                <td>{task.last_run ? new Date(task.last_run).toLocaleString() : 'Never'}</td>
                                <td>{task.next_run ? new Date(task.next_run).toLocaleString() : 'Pending'}</td>
                                <td>
                                    <span className={`status-${task.enabled ? 'SUCCESS' : 'FAILURE'}`}>
                                        {task.enabled ? 'Active' : 'Disabled'}
                                    </span>
                                </td>
                                <td>
                                    <button className="btn btn-secondary btn-sm" onClick={() => handleRunNow(task.task_id)}>Run Now</button>
                                    <button className="btn btn-secondary btn-sm" onClick={() => handleEdit(task)}>Edit</button>
                                    <button className="btn btn-danger btn-sm" onClick={() => handleDelete(task.task_id)}>Delete</button>
                                </td>
                            </tr>
                        ))}
                        {tasks.length === 0 && (
                            <tr>
                                <td colSpan="7" style={{ textAlign: 'center', padding: '2rem' }}>No tasks found. Create one to get started.</td>
                            </tr>
                        )}
                    </tbody>
                </table>
            </section>

            {showModal && (
                <TaskModal
                    task={editingTask}
                    jobs={jobs}
                    onClose={() => setShowModal(false)}
                    onSave={handleSave}
                />
            )}
        </div>
    );
}

export default TaskTab;
