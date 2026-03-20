// Fairplay App Shell - shared across all app pages
let currentUser = null;

// Grab token from URL hash (after magic link redirect) and store in localStorage
(function() {
    const hash = window.location.hash;
    if (hash && hash.includes('token=')) {
        const token = hash.split('token=')[1].split('&')[0];
        if (token) {
            API.setToken(token);
            // Clean the URL
            history.replaceState(null, '', window.location.pathname + window.location.search);
        }
    }
})();

async function initApp() {
    try {
        currentUser = await API.getMe();
        renderNav();

        // If no connections, redirect to onboarding (unless already there)
        if (!currentUser.has_connections && !window.location.pathname.includes('onboarding')) {
            window.location.href = '/static/onboarding.html';
            return;
        }
    } catch (e) {
        window.location.href = '/static/login.html';
    }
}

function renderNav() {
    const nav = document.getElementById('app-nav');
    if (!nav) return;

    const page = window.location.pathname.split('/').pop().replace('.html', '');

    nav.innerHTML = `
        <div class="nav-brand">
            <div class="nav-logo">F</div>
            <span>Fairplay</span>
        </div>
        <div class="nav-items">
            <a href="/static/dashboard.html" class="nav-item ${page === 'dashboard' ? 'active' : ''}">
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>
                Dashboard
            </a>
            <a href="/static/deals.html" class="nav-item ${page === 'deals' ? 'active' : ''}">
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14,2 14,8 20,8"/></svg>
                Deal Log
            </a>
            <a href="/static/settings.html" class="nav-item ${page === 'settings' ? 'active' : ''}">
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="3"/><path d="M12 1v2M12 21v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M1 12h2M21 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42"/></svg>
                Settings
            </a>
        </div>
        <div class="nav-footer">
            <div class="nav-user">${currentUser ? currentUser.email : ''}</div>
            <button class="nav-logout" onclick="handleLogout()">Sign Out</button>
        </div>
    `;
}

async function handleLogout() {
    await API.logout();
    window.location.href = '/static/login.html';
}

function formatDate(dateStr) {
    if (!dateStr) return '';
    const d = new Date(dateStr);
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}

function formatTime(dateStr) {
    if (!dateStr) return '';
    const d = new Date(dateStr);
    return d.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' });
}

function scoreColor(score) {
    if (score >= 70) return '#22c55e';
    if (score >= 50) return '#f59e0b';
    return '#ef4444';
}

function recBadge(rec) {
    const map = {
        'auto_create': { label: 'Deal Created', cls: 'badge-green' },
        'needs_review': { label: 'Needs Review', cls: 'badge-yellow' },
        'not_a_deal': { label: 'Not a Deal', cls: 'badge-red' },
    };
    const m = map[rec] || { label: rec, cls: 'badge-gray' };
    return `<span class="badge ${m.cls}">${m.label}</span>`;
}
