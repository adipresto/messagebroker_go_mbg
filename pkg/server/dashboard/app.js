const { Subject, fromEvent, merge, of } = rxjs;
const { map, scan, filter, switchMap, catchError, retry, tap, delay } = rxjs.operators;

const socketUrl = `ws://${window.location.host}/ws`;
const statusBadge = document.getElementById('status-badge');
const queueCount = document.getElementById('queue-count');
const lastUpdate = document.getElementById('last-update');
const logContainer = document.getElementById('log-container');

// Reactive WebSocket Subject
const socket$ = new Subject();
let socket;

function connect() {
    socket = new WebSocket(socketUrl);

    socket.onopen = () => {
        statusBadge.textContent = 'ONLINE';
        statusBadge.className = 'badge online';
        addLog('System', 'WebSocket connected', 'system');
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
        const now = new Date();
        lastUpdate.textContent = now.toLocaleTimeString();
        addLog('Broker', `Queue updated: ${data.queue_size} messages`, 'pop');
    })
).subscribe();

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
