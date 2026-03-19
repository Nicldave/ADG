// Fairplay API Client
const API = {
    base: window.location.origin,

    async request(method, path, body = null) {
        const opts = {
            method,
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
        };
        if (body) opts.body = JSON.stringify(body);

        const res = await fetch(this.base + path, opts);

        if (res.status === 401) {
            window.location.href = '/static/login.html';
            return null;
        }

        if (!res.ok) {
            const err = await res.json().catch(() => ({ detail: res.statusText }));
            throw new Error(err.detail || 'Request failed');
        }

        return res.json();
    },

    get(path) { return this.request('GET', path); },
    post(path, body) { return this.request('POST', path, body); },
    del(path) { return this.request('DELETE', path); },

    // Auth
    sendMagicLink(email) { return this.post('/auth/magic-link', { email }); },
    getMe() { return this.get('/auth/me'); },
    logout() { return this.post('/auth/logout'); },

    // Connections
    getConnections() { return this.get('/connections'); },
    createConnection(data) { return this.post('/connections', data); },
    deleteConnection(webhookId) { return this.del(`/connections/${webhookId}`); },

    // Deals & Scoring
    getDeals() { return this.get('/deals'); },
    getStats() { return this.get('/dashboard/stats'); },
    getFrameworks() { return this.get('/frameworks'); },
    processLatest() { return this.post('/process-latest'); },
    batchScore(count) { return this.post('/batch-score', { count }); },
};
