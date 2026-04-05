const { Subject } = rxjs;
const { tap } = rxjs.operators;

const socketUrl = `ws://${window.location.host}/ws`;
const statusBadge = document.getElementById('status-badge');
const queueCount = document.getElementById('queue-count');
const dlqCount = document.getElementById('dlq-count');
const navDlqBadge = document.getElementById('nav-dlq-badge');
const logContainer = document.getElementById('log-container');
const targetsBody = document.getElementById('targets-body');

// Modal Elements
const targetModal = document.getElementById('target-modal');
const addTargetBtn = document.getElementById('add-target-btn');
const closeModalBtn = document.getElementById('close-modal');
const cancelModalBtn = document.getElementById('cancel-modal');
const targetForm = document.getElementById('target-form');
const addHeaderBtn = document.getElementById('add-header-row');
const headersContainer = document.getElementById('headers-container');

let editingTargetName = null;

function loadTargets() {
    fetch('/api/targets')
        .then(response => response.json())
        .then(targets => renderTargets(targets))
        .catch(err => {
            console.error('Failed to load targets:', err);
            if (targetsBody) {
                targetsBody.innerHTML = '<tr><td colspan="4" class="empty-state" style="color: var(--error);">Error loading targets. Ensure the broker is running.</td></tr>';
            }
        });
}

function renderTargets(targets) {
    if (!targetsBody) return;
    targetsBody.innerHTML = '';
    
    if (!targets || targets.length === 0) {
        targetsBody.innerHTML = '<tr><td colspan="4" class="empty-state">No targets registered yet.</td></tr>';
        return;
    }

    // Sort targets by name to ensure stable UI during updates
    targets.sort((a, b) => a.name.localeCompare(b.name));

    targets.forEach(target => {
        const isEditing = editingTargetName === target.name;
        const row = document.createElement('tr');
        if (isEditing) row.className = 'editing-row';
        
        // Name Cell (Read-only)
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
        
        // URL Cell (Input if editing)
        const urlCell = document.createElement('td');
        if (isEditing) {
            urlCell.innerHTML = `<input type="text" class="inline-edit-input" value="${target.url}" id="edit-url-${target.name}">`;
        } else {
            urlCell.innerHTML = `<a href="${target.url}" target="_blank" class="td-url" style="font-family: 'JetBrains Mono', monospace; font-size: 13px;">${target.url}</a>`;
        }
        row.appendChild(urlCell);
        
        // Config Cell
        const headersCell = document.createElement('td');
        if (isEditing) {
            const container = document.createElement('div');
            container.className = 'inline-headers-container';
            container.id = `edit-headers-${target.name}`;
            
            if (target.headers) {
                Object.entries(target.headers).forEach(([k, v]) => {
                    container.appendChild(createInlineHeaderRow(k, v));
                });
            }
            
            headersCell.appendChild(container);
            
            const addBtn = document.createElement('button');
            addBtn.className = 'btn-inline-add';
            addBtn.innerHTML = `
                <svg width="12" height="12" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M12 4v16m8-8H4"></path></svg>
                Add Header
            `;
            addBtn.onclick = () => container.appendChild(createInlineHeaderRow('', ''));
            headersCell.appendChild(addBtn);
        } else if (target.headers && Object.keys(target.headers).length > 0) {
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

        // Actions Cell
        const actionsCell = document.createElement('td');
        actionsCell.className = 'td-actions';
        if (isEditing) {
            actionsCell.innerHTML = `
                <button class="inline-action-btn save" title="Save" onclick="saveInlineEdit('${target.name}')">
                    <svg width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7"></path></svg>
                </button>
                <button class="inline-action-btn cancel" title="Cancel" onclick="exitEditMode()">
                    <svg width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12"></path></svg>
                </button>
            `;
        } else {
            actionsCell.innerHTML = `
                <button class="inline-action-btn edit" title="Edit URL" onclick="enterEditMode('${target.name}')">
                     <svg width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M15.232 5.232l3.536 3.536m-2.036-5.036a2.5 2.5 0 113.536 3.536L6.5 21.036H3v-3.572L16.732 3.732z"></path></svg>
                </button>
                <button class="inline-action-btn config" title="Configuration" onclick="openConfigModal('${target.name}')">
                     <svg width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"></path></svg>
                </button>
            `;
        }
        row.appendChild(actionsCell);
        
        targetsBody.appendChild(row);
    });
}

function enterEditMode(name) {
    editingTargetName = name;
    loadTargets();
}

function exitEditMode() {
    editingTargetName = null;
    loadTargets();
}

function createInlineHeaderRow(key, value) {
    const row = document.createElement('div');
    row.className = 'inline-header-row';
    row.innerHTML = `
        <input type="text" class="mini-edit-input key" placeholder="Key" value="${key}">
        <input type="text" class="mini-edit-input value" placeholder="Value" value="${value}">
        <button class="btn-inline-remove" title="Remove">
            <svg width="12" height="12" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12"></path></svg>
        </button>
    `;
    row.querySelector('.btn-inline-remove').onclick = () => row.remove();
    return row;
}

function saveInlineEdit(name) {
    const urlInput = document.getElementById(`edit-url-${name}`);
    const headersContainer = document.getElementById(`edit-headers-${name}`);
    if (!urlInput) return;
    
    const newUrl = urlInput.value.trim();
    if (!newUrl) return;

    const newHeaders = {};
    if (headersContainer) {
        headersContainer.querySelectorAll('.inline-header-row').forEach(row => {
            const k = row.querySelector('.key').value.trim();
            const v = row.querySelector('.value').value.trim();
            if (k) newHeaders[k] = v;
        });
    }

    fetch('/api/targets', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, url: newUrl, headers: newHeaders })
    })
    .then(res => {
        if (!res.ok) throw new Error('Failed to update target');
        addLog('System', `Target "${name}" updated inline (URL & Headers)`, 'push');
        exitEditMode();
    })
    .catch(err => {
        addLog('Error', `Inline update failed: ${err.message}`, 'dlq');
        alert(err.message);
    });
}

function openConfigModal(name) {
    fetch('/api/targets')
        .then(res => res.json())
        .then(targets => {
            const target = targets.find(t => t.name === name);
            if (!target) return;
            
            // Fill form
            document.getElementById('target-name').value = target.name;
            document.getElementById('target-url').value = target.url;
            headersContainer.innerHTML = '';
            
            if (target.headers) {
                Object.entries(target.headers).forEach(([k, v]) => {
                    const row = document.createElement('div');
                    row.className = 'header-row';
                    row.innerHTML = `
                        <input type="text" placeholder="Key" class="header-key" value="${k}">
                        <input type="text" placeholder="Value" class="header-value" value="${v}">
                        <button type="button" class="btn-remove-header">
                            <svg width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12"></path></svg>
                        </button>
                    `;
                    row.querySelector('.btn-remove-header').addEventListener('click', () => row.remove());
                    headersContainer.appendChild(row);
                });
            }
            
            targetModal.style.display = 'flex';
            document.body.style.overflow = 'hidden';
            document.querySelector('.modal-title').textContent = 'Edit Target Configuration';
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

// Modal Logic
if (addTargetBtn) {
    addTargetBtn.addEventListener('click', () => {
        targetModal.style.display = 'flex';
        document.body.style.overflow = 'hidden';
    });
}

function hideModal() {
    targetModal.style.display = 'none';
    document.body.style.overflow = 'auto';
    targetForm.reset();
    headersContainer.innerHTML = '';
}

[closeModalBtn, cancelModalBtn].forEach(btn => {
    if (btn) btn.addEventListener('click', hideModal);
});

targetModal.addEventListener('click', (e) => {
    if (e.target === targetModal) hideModal();
});

if (addHeaderBtn) {
    addHeaderBtn.addEventListener('click', () => {
        const row = document.createElement('div');
        row.className = 'header-row';
        row.innerHTML = `
            <input type="text" placeholder="Key" class="header-key">
            <input type="text" placeholder="Value" class="header-value">
            <button type="button" class="btn-remove-header">
                <svg width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12"></path></svg>
            </button>
        `;
        
        row.querySelector('.btn-remove-header').addEventListener('click', () => row.remove());
        headersContainer.appendChild(row);
    });
}

if (targetForm) {
    targetForm.addEventListener('submit', (e) => {
        e.preventDefault();
        
        const name = document.getElementById('target-name').value;
        const url = document.getElementById('target-url').value;
        
        const headers = {};
        headersContainer.querySelectorAll('.header-row').forEach(row => {
            const key = row.querySelector('.header-key').value.trim();
            const value = row.querySelector('.header-value').value.trim();
            if (key) headers[key] = value;
        });

        const submitBtn = targetForm.querySelector('.btn-submit');
        const originalText = submitBtn.textContent;
        submitBtn.disabled = true;
        submitBtn.textContent = 'Registering...';

        fetch('/api/targets', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name, url, headers })
        })
        .then(async response => {
            if (!response.ok) {
                const text = await response.text();
                throw new Error(text || 'Failed to register target');
            }
            return response.json();
        })
        .then(data => {
            addLog('System', `Target "${name}" registered successfully`, 'push');
            loadTargets();
            hideModal();
        })
        .catch(err => {
            console.error('Registration failed:', err);
            addLog('Error', `Failed to register target: ${err.message}`, 'dlq');
            alert(`Error: ${err.message}`);
        })
        .finally(() => {
            submitBtn.disabled = false;
            submitBtn.textContent = originalText;
        });
    });
}
