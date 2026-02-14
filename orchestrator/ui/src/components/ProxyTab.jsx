import React, { useState, useEffect } from 'react';
import axios from 'axios';

function ProxyTab() {
    const [status, setStatus] = useState(null);
    const [loading, setLoading] = useState(false);
    const [showConfig, setShowConfig] = useState(false);

    // Config Form State
    const [config, setConfig] = useState({
        concurrency: 50,
        ttl: 300,
        target_pool_size: 100,
        test_url: '',
        sources: []
    });

    const fetchStatus = () => {
        axios.get('/api/proxies/status').then(res => {
            setStatus(res.data);
            if (!showConfig && res.data.config) {
                setConfig(res.data.config);
            }
        }).catch(err => console.error("Failed to fetch proxy status", err));
    };

    // Helper to determine status display
    const isFull = status ? status.active_proxies >= (status.config.target_pool_size || 100) : false;

    useEffect(() => {
        fetchStatus();
        const interval = setInterval(fetchStatus, 3000);
        return () => clearInterval(interval);
    }, []);

    const handleRefresh = () => {
        setLoading(true);
        axios.post('/api/proxies/refresh').then(() => {
            setLoading(false);
            fetchStatus();
        }).catch(err => {
            alert("Error triggering refresh");
            setLoading(false);
        });
    };

    const handleSaveConfig = () => {
        axios.post('/api/proxies/config', config).then(() => {
            setShowConfig(false);
            fetchStatus();
        }).catch(err => alert("Failed to save config: " + err));
    };

    if (!status) return <div className="p-8 text-center text-gray-400">Loading Proxy Manager...</div>;

    return (
        <div className="glass-panel animate-fade-in">
            {/* Header Section */}
            <div className="flex justify-between items-center mb-8">
                <div>
                    <h2 className="text-3xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-400 to-purple-500">
                        Proxy Network
                    </h2>
                    <p className="text-gray-400 text-sm mt-1">Smart Pool Manager (Target: {status.config.target_pool_size || 50})</p>
                </div>
                <div className="flex gap-4">
                    <button
                        onClick={() => setShowConfig(!showConfig)}
                        className="px-4 py-2 rounded-full border border-gray-600 hover:bg-gray-700 transition"
                    >
                        {showConfig ? 'Close Config' : '⚙️ Configure'}
                    </button>
                    <button
                        onClick={handleRefresh}
                        className="px-6 py-2 rounded-full font-bold shadow-lg transition transform hover:-translate-y-1 bg-gradient-to-r from-blue-500 to-cyan-600 text-white"
                    >
                        ☁ Force Source Update
                    </button>
                </div>
            </div>

            {/* Stats Grid */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
                <div className="glass-panel stat-card text-center p-6">
                    <div className="text-gray-400 text-xs font-bold uppercase tracking-widest mb-2">Active Nodes</div>
                    <div className="text-5xl font-black text-transparent bg-clip-text bg-gradient-to-b from-green-300 to-green-600">
                        {status.active_proxies}
                    </div>
                    <div className="text-xs text-green-400 mt-2">Verified Working</div>
                </div>

                <div className="glass-panel stat-card text-center p-6">
                    <div className="text-gray-400 text-xs font-bold uppercase tracking-widest mb-2">Status</div>
                    <div className={`px-4 py-1 rounded-full text-sm font-bold ${isFull ? 'bg-blue-500/20 text-blue-300' : 'bg-yellow-500/20 text-yellow-300 animate-pulse'}`}>
                        {isFull ? 'POOL FULL (IDLE)' : 'REFILLING...'}
                    </div>
                    <div className="text-xs text-gray-500 mt-3 font-mono">
                        Updated: {status.last_updated ? new Date(status.last_updated * 1000).toLocaleTimeString() : 'Never'}
                    </div>
                </div>

                <div className="glass-panel stat-card text-center p-6">
                    <div className="text-gray-400 text-xs font-bold uppercase tracking-widest mb-2">Churn Rate</div>
                    <div className="text-4xl font-bold text-purple-400">
                        {status.churn_stats ? (status.churn_stats.checked).toLocaleString() : 0}
                    </div>
                    <div className="text-xs text-gray-500 mt-2">Checks Performed</div>
                </div>

                <div className="glass-panel stat-card text-center p-6 flex flex-col justify-center items-center">
                    <div className="text-gray-400 text-xs font-bold uppercase tracking-widest mb-2">Total Pool</div>
                    <div className="text-4xl font-bold text-blue-400">
                        {(status.total_raw / 1000).toFixed(1)}k
                    </div>
                    <div className="text-xs text-gray-500 mt-2">Raw Candidates</div>
                </div>
            </div>

            {/* Configuration Panel */}
            {showConfig && (
                <div className="glass-panel mt-6 border border-blue-500/30 shadow-2xl animate-fade-in-up">
                    <h3 className="text-xl font-bold mb-6 text-blue-400 flex items-center gap-2">
                        🔧 Network Configuration
                    </h3>

                    <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-6">
                        <div>
                            <label className="block text-xs font-bold text-gray-400 uppercase mb-2">Target Pool Size</label>
                            <input
                                type="number"
                                className="w-full bg-gray-900 border border-gray-700 rounded-lg px-4 py-2 text-white focus:border-blue-500 focus:outline-none"
                                value={config.target_pool_size}
                                onChange={e => setConfig({ ...config, target_pool_size: parseInt(e.target.value) })}
                            />
                        </div>
                        <div>
                            <label className="block text-xs font-bold text-gray-400 uppercase mb-2">Concurrency Limit</label>
                            <input
                                type="number"
                                className="w-full bg-gray-900 border border-gray-700 rounded-lg px-4 py-2 text-white focus:border-blue-500 focus:outline-none"
                                value={config.concurrency}
                                onChange={e => setConfig({ ...config, concurrency: parseInt(e.target.value) })}
                            />
                        </div>
                        <div>
                            <label className="block text-xs font-bold text-gray-400 uppercase mb-2">Cache TTL (Seconds)</label>
                            <input
                                type="number"
                                className="w-full bg-gray-900 border border-gray-700 rounded-lg px-4 py-2 text-white focus:border-blue-500 focus:outline-none"
                                value={config.ttl}
                                onChange={e => setConfig({ ...config, ttl: parseInt(e.target.value) })}
                            />
                        </div>
                        <div>
                            <label className="block text-xs font-bold text-gray-400 uppercase mb-2">Validation Target</label>
                            <input
                                type="text"
                                className="w-full bg-gray-900 border border-gray-700 rounded-lg px-4 py-2 text-white font-mono text-sm focus:border-blue-500 focus:outline-none"
                                value={config.test_url}
                                onChange={e => setConfig({ ...config, test_url: e.target.value })}
                            />
                        </div>
                    </div>

                    <div className="mb-6">
                        <label className="block text-xs font-bold text-gray-400 uppercase mb-2">Source Lists (One URL per line)</label>
                        <textarea
                            className="premium-input h-64"
                            value={config.sources.join('\n')}
                            onChange={e => setConfig({ ...config, sources: e.target.value.split('\n').filter(s => s.trim()) })}
                            placeholder="https://example.com/proxy-list.txt"
                        />
                        <div className="text-right text-xs text-gray-500 mt-1">Found {config.sources.length} sources</div>
                    </div>

                    <div className="flex justify-end gap-4 border-t border-gray-700 pt-4">
                        <button
                            onClick={() => setShowConfig(false)}
                            className="px-6 py-2 text-gray-400 hover:text-white transition"
                        >
                            Cancel
                        </button>
                        <button
                            onClick={handleSaveConfig}
                            className="btn-premium shadow-lg shadow-blue-500/20"
                        >
                            Save Configuration
                        </button>
                    </div>
                </div>
            )}
        </div>
    );
}

export default ProxyTab;
