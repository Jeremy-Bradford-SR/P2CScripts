import React, { useState } from 'react';
import { Layers, Activity, Server, CalendarClock } from 'lucide-react';
import './App.css';

import IngestionDashboard from './components/IngestionDashboard';
import ProxyNetwork from './components/ProxyNetwork';
import ScheduledTasks from './components/ScheduledTasks';

export default function App() {
    const [activeTab, setActiveTab] = useState('ingestion');

    const tabs = [
        { id: 'ingestion', label: 'Ingestion Engine', icon: <Activity className="mobile-icon" size={24} /> },
        { id: 'proxies', label: 'Proxy Network', icon: <Server className="mobile-icon" size={24} /> },
        { id: 'tasks', label: 'Scheduled Tasks', icon: <CalendarClock className="mobile-icon" size={24} /> }
    ];

    return (
        <div className="app-container">
            <header className="top-header">
                <div className="header-content">
                    <div className="logo-wrap">
                        <div style={{ background: '#000', color: '#fff', padding: '4px', borderRadius: '4px', display: 'flex' }}>
                            <Layers size={16} />
                        </div>
                        <span className="logo-text">P2C Orchestrator</span>
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
                        <span style={{ fontSize: '13px', color: 'var(--accents-5)' }}>City of Dubuque</span>
                    </div>
                </div>
            </header>

            {/* Desktop Sub Navigation */}
            <nav className="sub-nav">
                <div className="sub-nav-content">
                    {tabs.map(t => (
                        <div
                            key={t.id}
                            className={`tab-link ${activeTab === t.id ? 'active' : ''}`}
                            onClick={() => setActiveTab(t.id)}
                        >
                            {t.label}
                        </div>
                    ))}
                </div>
            </nav>

            {/* Mobile Bottom Navigation */}
            <nav className="bottom-mobile-nav">
                {tabs.map(t => (
                    <div
                        key={t.id}
                        className={`nav-item-mobile ${activeTab === t.id ? 'active' : ''}`}
                        onClick={() => setActiveTab(t.id)}
                    >
                        {React.cloneElement(t.icon, { color: activeTab === t.id ? '#000' : '#888' })}
                        <span>{t.label}</span>
                    </div>
                ))}
            </nav>

            <main className="main-wrapper">
                {activeTab === 'ingestion' && <IngestionDashboard />}
                {activeTab === 'proxies' && <ProxyNetwork />}
                {activeTab === 'tasks' && <ScheduledTasks />}
            </main>
        </div>
    );
}
