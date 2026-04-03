const { Subject, fromEvent, merge, of } = rxjs;
const { map, scan, filter, switchMap, catchError, retry, tap, delay } = rxjs.operators;

const socketUrl = `ws://${window.location.host}/ws`;
const statusBadge = document.getElementById('status-badge');
const queueCount = document.getElementById('queue-count');
const dlqCount = document.getElementById('dlq-count');
const lastUpdate = document.getElementById('last-update');
const logContainer = document.getElementById('log-container');
const targetsBody = document.getElementById('targets-body');

function loadTargets() {
    fetch('/api/targets')
        .then(response => response.json())
        .then(targets => renderTargets(targets))
        .catch(err => {
            console.error('Failed to load targets:', err);
            if (targetsBody) {
                targetsBody.innerHTML = '<tr><td colspan="3" style="text-align: center; color: var(--error); padding: 1.5rem;">Error loading targets</td></tr>';
            }
        });
}

function renderTargets(targets) {
    if (!targetsBody) return;
    targetsBody.innerHTML = '';
    if (!targets || targets.length === 0) {
        targetsBody.innerHTML = '<tr><td colspan="3" style="text-align: center; color: var(--text-secondary); padding: 1.5rem;">No targets registered</td></tr>';
        return;
    }
    targets.forEach(target => {
        const row = document.createElement('tr');
        
        const nameCell = document.createElement('td');
        nameCell.textContent = target.name;
        nameCell.style.fontWeight = '600';
        row.appendChild(nameCell);
        
        const urlCell = document.createElement('td');
        urlCell.textContent = target.url;
        urlCell.style.fontFamily = "'JetBrains Mono', monospace";
        urlCell.style.fontSize = "0.875rem";
        row.appendChild(urlCell);
        
        const headersCell = document.createElement('td');
        if (target.headers && Object.keys(target.headers).length > 0) {
            Object.entries(target.headers).forEach(([k, v]) => {
                const tag = document.createElement('span');
                tag.className = 'header-tag';
                tag.textContent = `${k}: ${v}`;
                headersCell.appendChild(tag);
            });
        } else {
            headersCell.innerHTML = '<span style="color: var(--text-secondary); opacity: 0.5;">None</span>';
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
        statusBadge.textContent = 'ONLINE';
        statusBadge.className = 'badge online';
        addLog('System', 'WebSocket connected', 'system');
        loadTargets();
    };

    socket.onmessage = (event) => {
        const data = JSON.parse(event.data);
        socket$.next(data);
    };

    socket.onclose = () => {
        statusBadge.textContent = 'OFFLINE';
        statusBadge.className = 'badge offline';
        addLog('System', 'WebSocket disconnected, retrying...', 'system');
        setTimeout(connect, 3000); // Simple retry
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
            dlqCount.classList.remove('warning');
            dlqCount.style.color = 'var(--error)';
        } else {
            dlqCount.classList.add('warning');
            dlqCount.style.color = '';
        }

        // Update CB Statuses
        updateCBStatus('storage', data.storage_cb);
        updateCBStatus('network', data.network_cb);

        // [Reactive] Update targets if provided in the stream
        if (data.targets) {
            renderTargets(data.targets);
        }
        
        // Log update only if data changed
        addLog('Broker', `Update received - Q: ${data.queue_size}, DLQ: ${data.dlq_size}, Storage: ${data.storage_cb.state}, Net: ${data.network_cb.state}`, 'pop');
    })
).subscribe();

function updateCBStatus(prefix, cb) {
    const stateEl = document.getElementById(`${prefix}-cb-state`);
    const infoEl = document.getElementById(`${prefix}-cb-info`);
    const cardEl = document.getElementById(`${prefix}-cb-card`);

    if (!stateEl || !cb) return;

    stateEl.textContent = cb.state.toUpperCase();
    infoEl.textContent = `${cb.failures}/${cb.threshold} failures`;

    // Visual feedback for Open/Half-Open
    cardEl.style.borderColor = 'rgba(255, 255, 255, 0.05)';
    stateEl.style.color = 'var(--accent-color)';

    if (cb.state === 'Open') {
        stateEl.style.color = 'var(--error)';
        cardEl.style.borderColor = 'var(--error)';
    } else if (cb.state === 'Half-Open') {
        stateEl.style.color = 'var(--warning)';
        cardEl.style.borderColor = 'var(--warning)';
    } else if (cb.state === 'Closed') {
        stateEl.style.color = 'var(--success)';
    }
}

function addLog(source, message, type) {
    const entry = document.createElement('div');
    entry.className = `log-entry ${type}`;
    const timestamp = new Date().toLocaleTimeString();
    entry.textContent = `[${timestamp}] [${source}] ${message}`;
    logContainer.prepend(entry);
    
    // Max 100 entries
    if (logContainer.children.length > 100) {
        logContainer.lastChild.remove();
    }
}

// Initial connection
connect();
