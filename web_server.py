# ============================================================
#   WEB BRIDGE SERVER — Flask + Socket.IO
#   Bridges browser WebSocket connections to the existing
#   threaded SSL socket server (server_modified.py)
#
#   Usage:
#     1. Start server_modified.py first
#     2. Run: python web_server.py
#     3. Open http://localhost:5000 in browser
# ============================================================

import socket
import ssl
import threading
import os
from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit

# ─────────────────────────────────────
# CONFIGURATION  (must match server_modified.py)
# ─────────────────────────────────────

HEADER    = 64
PORT      = 8080                     # socket server port
SERVER    = "192.168.31.247"              # socket server IP
FORMAT    = 'utf-8'
CERT_FILE = "cert.pem"

WEB_PORT  = 5000                     # web interface port

# Command keywords (must match server)
DISCONNECT_MESSAGE = "!DISCONNECT"
FILE_MESSAGE       = "!FILE"
USERS_MESSAGE      = "!USERS"
DM_MESSAGE         = "!DM"
REGISTER_MESSAGE   = "!REGISTER"
LOGIN_MESSAGE      = "!LOGIN"

# ─────────────────────────────────────
# FLASK APP
# ─────────────────────────────────────

app = Flask(__name__)
app.config['SECRET_KEY'] = os.urandom(24).hex()
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Store active sessions:  sid -> { 'socket': ssl_socket, 'username': str, 'listener': Thread }
active_sessions = {}
sessions_lock   = threading.Lock()


# ─────────────────────────────────────
# PROTOCOL HELPERS
# ─────────────────────────────────────

def send_to_server(sock, msg):
    """Send a message using the 64-byte header protocol."""
    encoded    = msg.encode(FORMAT)
    header     = str(len(encoded)).encode(FORMAT).ljust(HEADER)
    sock.send(header)
    sock.send(encoded)


def recv_from_server(sock):
    """Receive a message using the 64-byte header protocol."""
    try:
        raw_header = sock.recv(HEADER)
        if not raw_header:
            return None
        msg_length = raw_header.decode(FORMAT).strip()
        if not msg_length:
            return None
        msg = sock.recv(int(msg_length)).decode(FORMAT)
        return msg
    except:
        return None


def create_ssl_socket():
    """Create an SSL socket connection to the chat server."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(CERT_FILE)
    ctx.check_hostname = False

    raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ssl_sock = ctx.wrap_socket(raw, server_hostname=SERVER)
    ssl_sock.connect((SERVER, PORT))
    return ssl_sock


# ─────────────────────────────────────
# LISTENER THREAD
# ─────────────────────────────────────

def listener_thread(sid):
    """Background thread that reads messages from the socket
       server and forwards them to the browser via Socket.IO."""
    with sessions_lock:
        session = active_sessions.get(sid)
    if not session:
        return

    sock = session['socket']

    while True:
        msg = recv_from_server(sock)
        if msg is None:
            # Connection lost
            socketio.emit('server_message', {
                'type': 'system',
                'message': '🔴 Lost connection to server.'
            }, room=sid)
            break

        # Skip file transfer signals (not supported in web)
        if msg == FILE_MESSAGE:
            # Consume the file metadata to keep the stream in sync
            _skip_file_data(sock)
            continue

        # Classify the message
        msg_type = 'other'
        if '[SERVER]' in msg:
            msg_type = 'server'
        elif '[PRIVATE' in msg:
            msg_type = 'private'
        elif '[ERROR]' in msg:
            msg_type = 'error'
        elif '[ONLINE USERS]' in msg:
            msg_type = 'users'

        socketio.emit('server_message', {
            'type': msg_type,
            'message': msg
        }, room=sid)

    # Cleanup on disconnect
    _cleanup_session(sid)


def _skip_file_data(sock):
    """If a FILE message arrives, skip the file metadata + data
       so the protocol stream stays in sync."""
    try:
        filename = recv_from_server(sock)
        filesize_str = recv_from_server(sock)
        if filesize_str:
            filesize = int(filesize_str)
            received = 0
            while received < filesize:
                chunk = sock.recv(min(4096, filesize - received))
                if not chunk:
                    break
                received += len(chunk)
    except:
        pass


def _cleanup_session(sid):
    """Remove a session from tracking."""
    with sessions_lock:
        session = active_sessions.pop(sid, None)
    if session and session.get('socket'):
        try:
            session['socket'].close()
        except:
            pass


# ─────────────────────────────────────
# ROUTES
# ─────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')


# ─────────────────────────────────────
# SOCKET.IO EVENTS
# ─────────────────────────────────────

@socketio.on('connect')
def handle_connect():
    print(f"[WEB] Browser connected: {request.sid}")


@socketio.on('disconnect')
def handle_disconnect():
    sid = request.sid
    print(f"[WEB] Browser disconnected: {sid}")

    with sessions_lock:
        session = active_sessions.get(sid)

    if session and session.get('socket'):
        try:
            send_to_server(session['socket'], DISCONNECT_MESSAGE)
        except:
            pass
    _cleanup_session(sid)


@socketio.on('auth')
def handle_auth(data):
    """Handle login or register from browser."""
    sid      = request.sid
    mode     = data.get('mode', 'login')          # 'login' or 'register'
    username = data.get('username', '').strip()
    password = data.get('password', '').strip()

    if not username or not password:
        emit('auth_response', {'success': False, 'message': 'Please fill in both fields.'})
        return

    try:
        sock = create_ssl_socket()
    except Exception as e:
        emit('auth_response', {'success': False, 'message': f'Cannot connect to chat server: {e}'})
        return

    try:
        cmd = REGISTER_MESSAGE if mode == 'register' else LOGIN_MESSAGE
        send_to_server(sock, cmd)
        send_to_server(sock, username)
        send_to_server(sock, password)

        response = recv_from_server(sock)

        if response and 'successful' in response.lower():
            # Store session
            with sessions_lock:
                active_sessions[sid] = {
                    'socket':   sock,
                    'username': username,
                    'listener': None
                }

            # Start listener thread
            t = threading.Thread(target=listener_thread, args=(sid,), daemon=True)
            with sessions_lock:
                active_sessions[sid]['listener'] = t
            t.start()

            emit('auth_response', {'success': True, 'message': response, 'username': username})
        else:
            sock.close()
            emit('auth_response', {'success': False, 'message': response or 'Authentication failed.'})

    except Exception as e:
        sock.close()
        emit('auth_response', {'success': False, 'message': f'Auth error: {e}'})


@socketio.on('send_message')
def handle_send_message(data):
    """Forward a chat message from browser to socket server."""
    sid = request.sid
    msg = data.get('message', '').strip()

    if not msg:
        return

    with sessions_lock:
        session = active_sessions.get(sid)

    if not session or not session.get('socket'):
        emit('server_message', {'type': 'error', 'message': '[ERROR] Not connected to server.'})
        return

    try:
        send_to_server(session['socket'], msg)
    except Exception as e:
        emit('server_message', {'type': 'error', 'message': f'[ERROR] Failed to send: {e}'})


@socketio.on('request_users')
def handle_request_users():
    """Request the online users list from the server."""
    sid = request.sid

    with sessions_lock:
        session = active_sessions.get(sid)

    if session and session.get('socket'):
        try:
            send_to_server(session['socket'], USERS_MESSAGE)
        except:
            pass


@socketio.on('leave')
def handle_leave():
    """Gracefully disconnect from the chat server."""
    sid = request.sid

    with sessions_lock:
        session = active_sessions.get(sid)

    if session and session.get('socket'):
        try:
            send_to_server(session['socket'], DISCONNECT_MESSAGE)
        except:
            pass

    _cleanup_session(sid)
    emit('left', {'message': 'You have left the chat.'})


# ─────────────────────────────────────
# START
# ─────────────────────────────────────

if __name__ == '__main__':
    print("=" * 50)
    print("  🌐 SecureChat Web Interface")
    print(f"  Open http://localhost:{WEB_PORT} in your browser")
    print(f"  Bridging to socket server at {SERVER}:{PORT}")
    print("=" * 50)
    socketio.run(app, host='0.0.0.0', port=WEB_PORT, debug=False, allow_unsafe_werkzeug=True)
