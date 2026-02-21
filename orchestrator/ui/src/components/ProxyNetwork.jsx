import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Activity, Zap, HardDrive, RefreshCcw } from 'lucide-react';
import { useInterval } from '../hooks/useInterval';

export default function ProxyNetwork() {
    const [proxyStats, setProxyStats] = useState(null);
    const [isRefreshing, setIsRefreshing] = useState(false);

    const [form, setForm] = useState({ concurrency: 250, target_pool_size: 100, sources: "" });
    const [saving, setSaving] = useState(false);

    const fetchProxies = async () => {
        try {
            const res = await axios.get('/api/proxies/status');
            setProxyStats(res.data);

            if (res.data?.config && document.activeElement.tagName !== 'TEXTAREA' && document.activeElement.tagName !== 'INPUT') {
                const c = res.data.config;
                setForm({
                    concurrency: c.concurrency || 250,
                    target_pool_size: c.target_pool_size || 100,
                    sources: c.sources ? c.sources.join('\n') : ""
                });
            }
        } catch (err) { }
    };

    useEffect(() => { fetchProxies(); }, []);
    useInterval(fetchProxies, 3000);

    const forceRefresh = async () => {
        setIsRefreshing(true);
        try {
            await axios.post('/api/proxies/refresh');
            await fetchProxies();
        } catch (e) { } finally { setIsRefreshing(false); }
    };

    const saveConfig = async () => {
        setSaving(true);
        try {
            const payload = {
                concurrency: parseInt(form.concurrency),
                ttl: 600,
                target_pool_size: parseInt(form.target_pool_size),
                sources: form.sources.split('\n').map(s => s.trim()).filter(s => s.length > 0)
            };
            await axios.put('/api/config/proxy_manager_config', payload);
            fetchProxies();
        } catch (e) { } finally { setSaving(false); }
    };

    if (!proxyStats) return null;

    return (
        <div style={{ animation: 'fadeIn 0.3s ease' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '32px' }}>
                <div>
                    <h2 className="page-title">Network Intelligence</h2>
                    <p className="page-desc">High-anonymity IP extraction & validation pool telemetry.</p>
                </div>
                <button className="btn btn-secondary" onClick={forceRefresh} disabled={isRefreshing}>
                    <RefreshCcw size={14} className={isRefreshing ? "animate-spin" : ""} style={{ marginRight: '6px' }} />
                    Force Rotation
                </button>
            </div>

            <div className="card-grid">
                <div className="v-card">
                    <div className="v-card-header">
                        <div className="v-card-title"><Activity size={18} color="var(--geist-success)" /> Available Targets</div>
                    </div>
                    <div className="v-card-body" style={{ fontSize: '48px', fontWeight: 700, letterSpacing: '-0.05em' }}>
                        {proxyStats.active_proxies}
                    </div>
                </div>
                <div className="v-card">
                    <div className="v-card-header">
                        <div className="v-card-title"><HardDrive size={18} /> Raw Endpoints</div>
                    </div>
                    <div className="v-card-body" style={{ fontSize: '48px', fontWeight: 700, letterSpacing: '-0.05em', color: 'var(--accents-6)' }}>
                        {proxyStats.total_raw}
                    </div>
                </div>
                <div className="v-card">
                    <div className="v-card-header">
                        <div className="v-card-title"><Zap size={18} color="var(--geist-warning)" /> Validation Yield</div>
                    </div>
                    <div className="v-card-body" style={{ fontSize: '48px', fontWeight: 700, letterSpacing: '-0.05em' }}>
                        {Math.round((proxyStats.churn_stats.success / (proxyStats.churn_stats.checked || 1)) * 100)}%
                    </div>
                </div>
            </div>

            <div className="v-card" style={{ marginTop: '48px', maxWidth: '800px' }}>
                <div className="v-card-header" style={{ borderBottom: '1px solid var(--accents-2)', marginBottom: '24px' }}>
                    <div className="v-card-title" style={{ fontSize: '18px' }}>Engine Parameters</div>
                    <p style={{ fontSize: '14px', color: 'var(--accents-5)' }}>Edit the live SQLite configuration driving the proxy scraper routine.</p>
                </div>
                <div className="v-card-body">
                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '24px' }}>
                        <div className="input-wrapper">
                            <label className="label">Concurrency Level</label>
                            <input className="input" type="number" value={form.concurrency} onChange={e => setForm({ ...form, concurrency: e.target.value })} />
                        </div>
                        <div className="input-wrapper">
                            <label className="label">Target Capacity</label>
                            <input className="input" type="number" value={form.target_pool_size} onChange={e => setForm({ ...form, target_pool_size: e.target.value })} />
                        </div>
                    </div>
                    <div className="input-wrapper" style={{ marginBottom: 0 }}>
                        <label className="label">Remote Data Sources (TXT IPs one per line)</label>
                        <textarea className="input" style={{ fontFamily: 'monospace' }} value={form.sources} onChange={e => setForm({ ...form, sources: e.target.value })} />
                    </div>
                </div>
                <div className="v-card-footer" style={{ justifyContent: 'flex-end', background: 'var(--geist-bg)' }}>
                    <button className="btn btn-primary" onClick={saveConfig} disabled={saving}>
                        {saving ? 'Applying...' : 'Apply Overrides'}
                    </button>
                </div>
            </div>
        </div>
    );
}
