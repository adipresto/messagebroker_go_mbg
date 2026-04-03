const { Subject } = rxjs;
const { tap } = rxjs.operators;

const socketUrl = `ws://${window.location.host}/ws`;
const statusBadge = document.getElementById('status-badge');
const queueCount = document.getElementById('queue-count');
const dlqCount = document.getElementById('dlq-count');
const navDlqBadge = document.getElementById('nav-dlq-badge');
const logContainer = document.getElementById('log-container');
const targetsBody = document.getElementById('targets-body');

function loadTargets() {
    fetch('/api/targets')
        .then(response => response.json())
        .then(targets => renderTargets(targets))
        .catch(err => {
            console.error('Failed to load targets:', err);
            if (targetsBody) {
                targetsBody.innerHTML = '<tr><td colspan="3" class="empty-state" style="color: var(--error);">Error loading targets. Ensure the broker is running.</td></tr>';
            }
        });
}

function renderTargets(targets) {
    if (!targetsBody) return;
    targetsBody.innerHTML = '';
    
    if (!targets || targets.length === 0) {
        targetsBody.innerHTML = '<tr><td colspan="3" class="empty-state">No targets registered yet.</td></tr>';
        return;
    }

    targets.forEach(target => {
        const row = document.createElement('tr');
        
        const nameCell = document.createElement('td');
        nameCell.innerHTML = `
            <div class="td-main">
                <div class="td-main-icon">
                    <svg width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"></path></svg>
                </div>
                <span>${target.name}</span>
            </div>
        `;
        row.appendChild(nameCell);
        
        const urlCell = document.createElement('td');
        urlCell.innerHTML = `<a href="${target.url}" target="_blank" class="td-url" style="font-family: 'JetBrains Mono', monospace; font-size: 13px;">${target.url}</a>`;
        row.appendChild(urlCell);
        
        const headersCell = document.createElement('td');
        if (target.headers && Object.keys(target.headers).length > 0) {
            Object.entries(target.headers).forEach(([k, v]) => {
                const tag = document.createElement('span');
                tag.className = 'pill blue';
                tag.style.marginRight = '8px';
                tag.style.marginBottom = '4px';
                tag.textContent = `${k}: ${v}`;
                headersCell.appendChild(tag);
            });
        } else {
            headersCell.innerHTML = '<span style="color: var(--text-muted); font-size: 13px;">No custom headers</span>';
        }
        row.appendChild(headersCell);
        
        targetsBody.appendChild(row);
    });
}

// Reactive WebSocket Subject
const socket$ = new Subject();
let socket;

function connect() {
    socket = new WebSocket(socketUrl);

    socket.onopen = () => {
        statusBadge.innerHTML = '<div class="status-dot"></div><span>Online</span>';
        statusBadge.className = 'status-indicator online';
        addLog('System', 'WebSocket connected successfully', 'system');
        loadTargets();
    };

    socket.onmessage = (event) => {
        const data = JSON.parse(event.data);
        socket$.next(data);
    };

    socket.onclose = () => {
        statusBadge.innerHTML = '<div class="status-dot"></div><span>Reconnecting</span>';
        statusBadge.className = 'status-indicator offline';
        addLog('System', 'WebSocket disconnected, returning...', 'system');
        setTimeout(connect, 3000);
    };

    socket.onerror = (error) => {
        console.error('WebSocket Error:', error);
    };
}

// Data Stream handling
socket$.pipe(
    tap(data => {
        queueCount.textContent = data.queue_size;
        dlqCount.textContent = data.dlq_size;
        
        if (data.dlq_size > 0) {
            dlqCount.className = 'stat-value danger';
            if (navDlqBadge) {
                navDlqBadge.style.display = 'block';
                navDlqBadge.textContent = data.dlq_size;
            }
        } else {
            dlqCount.className = 'stat-value highlight';
            if (navDlqBadge) {
                navDlqBadge.style.display = 'none';
            }
        }

        updateCBStatus('storage', data.storage_cb);
        updateCBStatus('network', data.network_cb);

        if (data.targets) {
            renderTargets(data.targets);
        }
        
        addLog('Broker', `Metrics synched - Q: ${data.queue_size}, DLQ: ${data.dlq_size}`, 'pop');
    })
).subscribe();

function updateCBStatus(prefix, cb) {
    const stateEl = document.getElementById(`${prefix}-cb-state`);
    const infoEl = document.getElementById(`${prefix}-cb-info`);
    const cardEl = document.getElementById(`${prefix}-cb-card`);

    if (!stateEl || !cb) return;

    stateEl.textContent = cb.state.toUpperCase();
    infoEl.textContent = `${cb.failures}/${cb.threshold} failures`;

    cardEl.style.borderColor = 'var(--border-color)';
    cardEl.style.boxShadow = 'var(--shadow-sm)';

    if (cb.state === 'Open') {
        stateEl.style.color = 'var(--error)';
        cardEl.style.borderColor = 'rgba(239, 68, 68, 0.4)';
        cardEl.style.boxShadow = '0 0 0 1px rgba(239, 68, 68, 0.2)';
    } else if (cb.state === 'Half-Open') {
        stateEl.style.color = 'var(--warning)';
        cardEl.style.borderColor = 'rgba(234, 179, 8, 0.4)';
        cardEl.style.boxShadow = '0 0 0 1px rgba(234, 179, 8, 0.2)';
    } else if (cb.state === 'Closed') {
        stateEl.style.color = 'var(--success)';
    }
}

function addLog(source, message, type) {
    const entry = document.createElement('div');
    entry.className = `log-entry ${type}`;
    const now = new Date();
    const timeStr = `${now.getHours().toString().padStart(2, '0')}:${now.getMinutes().toString().padStart(2, '0')}:${now.getSeconds().toString().padStart(2, '0')}`;
    
    entry.innerHTML = `<span class="log-time">[${timeStr}]</span> <span>[${source}] ${message}</span>`;
    logContainer.prepend(entry);
    
    // Max 100 entries limit to keep terminal fast
    if (logContainer.children.length > 100) {
        logContainer.lastChild.remove();
    }
}

// Initial connection
connect();
