import React, { useState, useEffect } from 'react';
import axios from 'axios';

function ProxyStatus() {
    const [status, setStatus] = useState(null);
    const [loading, setLoading] = useState(false);
    const [showConfig, setShowConfig] = useState(false);

    // Config Form State
    const [config, setConfig] = useState({
        concurrency: 50,
        ttl: 300,
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

    useEffect(() => {
        fetchStatus();
        const interval = setInterval(fetchStatus, 5000);
        return () => clearInterval(interval);
    }, []);

    const handleRefresh = () => {
        setLoading(true);
        axios.post('/api/proxies/refresh').then(() => {
            alert("Validation triggered!");
            setLoading(false);
            fetchStatus();
        }).catch(err => {
            alert("Error triggering refresh");
            setLoading(false);
        });
    };

    const handleSaveConfig = () => {
        axios.post('/api/proxies/config', config).then(() => {
            alert("Configuration saved!");
            setShowConfig(false);
            fetchStatus();
        }).catch(err => alert("Failed to save config: " + err));
    };

    if (!status) return <div className="p-4 bg-gray-800 rounded-lg animate-pulse h-32"></div>;

    return (
        <div className="bg-gray-800 p-4 rounded-lg shadow-lg border border-gray-700 text-white mb-6">
            <div className="flex justify-between items-center mb-4">
                <h2 className="text-xl font-bold flex items-center gap-2">
                    <span>🌐</span> Proxy Manager
                </h2>
                <div className="flex gap-2">
                    <button
                        onClick={() => setShowConfig(!showConfig)}
                        className="px-3 py-1 bg-gray-700 hover:bg-gray-600 rounded text-sm transition"
                    >
                        {showConfig ? 'Hide Config' : 'Configure'}
                    </button>
                    <button
                        onClick={handleRefresh}
                        disabled={status.is_validating || loading}
                        className={`px-3 py-1 rounded text-sm transition font-medium ${status.is_validating
                                ? 'bg-yellow-600 cursor-not-allowed animate-pulse'
                                : 'bg-green-600 hover:bg-green-500'
                            }`}
                    >
                        {status.is_validating ? 'Validating...' : 'Refresh Now'}
                    </button>
                </div>
            </div>

            {/* Stats Grid */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4">
                <div className="bg-gray-700 p-3 rounded text-center">
                    <div className="text-gray-400 text-xs uppercase tracking-wider">Active Proxies</div>
                    <div className="text-2xl font-bold text-green-400">{status.active_proxies}</div>
                    <div className="text-xs text-gray-500">Verified</div>
                </div>
                <div className="bg-gray-700 p-3 rounded text-center">
                    <div className="text-gray-400 text-xs uppercase tracking-wider">Total Raw</div>
                    <div className="text-2xl font-bold text-blue-400">{status.total_raw}</div>
                    <div className="text-xs text-gray-500">From Sources</div>
                </div>
                <div className="bg-gray-700 p-3 rounded text-center">
                    <div className="text-gray-400 text-xs uppercase tracking-wider">Status</div>
                    <div className={`text-lg font-bold ${status.is_validating ? 'text-yellow-400' : 'text-gray-300'}`}>
                        {status.is_validating ? 'RUNNING' : 'IDLE'}
                    </div>
                </div>
                <div className="bg-gray-700 p-3 rounded text-center">
                    <div className="text-gray-400 text-xs uppercase tracking-wider">Last Updated</div>
                    <div className="text-sm font-mono mt-1">
                        {status.last_updated ? new Date(status.last_updated * 1000).toLocaleTimeString() : 'Never'}
                    </div>
                </div>
            </div>

            {/* Configuration Panel */}
            {showConfig && (
                <div className="bg-gray-900 p-4 rounded border border-gray-600 mt-2">
                    <h3 className="font-bold mb-3 border-b border-gray-700 pb-2">Configuration</h3>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
                        <div>
                            <label className="block text-xs text-gray-400 mb-1">Concurrency (Threads)</label>
                            <input
                                type="number"
                                value={config.concurrency}
                                onChange={e => setConfig({ ...config, concurrency: parseInt(e.target.value) })}
                                className="w-full bg-gray-800 border border-gray-600 rounded px-2 py-1 text-white"
                            />
                        </div>
                        <div>
                            <label className="block text-xs text-gray-400 mb-1">TTL (Seconds)</label>
                            <input
                                type="number"
                                value={config.ttl}
                                onChange={e => setConfig({ ...config, ttl: parseInt(e.target.value) })}
                                className="w-full bg-gray-800 border border-gray-600 rounded px-2 py-1 text-white"
                            />
                        </div>
                        <div className="md:col-span-2">
                            <label className="block text-xs text-gray-400 mb-1">Test URL</label>
                            <input
                                type="text"
                                value={config.test_url}
                                onChange={e => setConfig({ ...config, test_url: e.target.value })}
                                className="w-full bg-gray-800 border border-gray-600 rounded px-2 py-1 text-white font-mono text-sm"
                            />
                        </div>
                        <div className="md:col-span-2">
                            <label className="block text-xs text-gray-400 mb-1">Sources (One URL per line)</label>
                            <textarea
                                value={config.sources.join('\n')}
                                onChange={e => setConfig({ ...config, sources: e.target.value.split('\n').filter(s => s.trim()) })}
                                className="w-full bg-gray-800 border border-gray-600 rounded px-2 py-1 text-white font-mono text-xs h-32"
                            />
                        </div>
                    </div>
                    <div className="flex justify-end gap-2">
                        <button onClick={() => setShowConfig(false)} className="px-3 py-1 text-gray-400 hover:text-white">Cancel</button>
                        <button onClick={handleSaveConfig} className="px-4 py-1 bg-blue-600 hover:bg-blue-500 rounded text-white">Save Configuration</button>
                    </div>
                </div>
            )}
        </div>
    );
}

export default ProxyStatus;
