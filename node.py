"""
UDP Overlay Networking Implementation
------------------------------------------------
Conforms to the provided skeleton and **does not change** any function signatures.
Students must implement all methods that raise NotImplementedError.

Behavior added (kept inside existing methods only):
- UDP broadcast discovery via BROADCAST_IP.
- Heartbeats with PING/PONG and simple RTT tracking.
- Peer table maintenance with timeouts.
- Log file per node: logs/<node_id>.log (no peer table in this log).
- Peer snapshot file per node: logs/<node_id>_peers_snapshot.txt.
- Log lines use a vertical bar after the tag, e.g., "[INFO] | ..." as requested.

Packet format (plain text):
    <TYPE>|<SEQ>|<FROM>|<IP>|<PORT>|<BODY>
Where BODY may contain JSON depending on TYPE.

Thread roles:
- listener(): receive and dispatch packets
- broadcaster(): send PEER_SYNC every SYNC_INTERVAL seconds
- heartbeat(): ping peers every PING_INTERVAL seconds; purge stale peers
- summary(): periodically write peers snapshot and console summary
"""

import socket
import threading
import time
import json
import os
from typing import Tuple


# global config
PORT = 5000
BROADCAST_IP = "192.168.0.255"
SYNC_INTERVAL = 5
PING_INTERVAL = 15
PING_TIMEOUT = 3
REMOVE_TIMEOUT = 30


class PeerNode:
    """
    Represents a single node in the decentralized chatbot overlay.
    Students will implement UDP broadcast discovery and heartbeat logic.
    """

    def __init__(self, node_id: str):
        """Initialize node state, socket, and peer table."""
        self.id = node_id
        self.ip = None
        self.port = PORT
        self.seq = 0
        self.sock = None
        # setting peer table
        self.peers = {}
        self.running = False
        self.lock = threading.Lock()

        # runtime helpers
        self._threads = []
        self._last_ping = {}
        self._pending = {}  

        # logs directory and paths
        os.makedirs("logs", exist_ok=True)
        self._log_path = None
        self._snapshot_path = None

    # setting up socket
    def _setup_socket(self):
        """Create and configure UDP socket for broadcast and unicast."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        except OSError:
            pass

        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except (AttributeError, OSError):
            pass
        sock.bind(("", self.port))
        # set timeout for recv
        sock.settimeout(1.0)
        self.sock = sock

    def _get_local_ip(self):
        """Return the local IP address of this host."""
        ip = "127.0.0.1"
        try:
            tmp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                tmp.connect(("8.8.8.8", 80))
                ip = tmp.getsockname()[0]
            finally:
                tmp.close()
        except Exception:
            ip = socket.gethostbyname(socket.gethostname())
        return ip

    # packet construction and sending
    def _next_seq(self):
        """Increment and return the next sequence number."""
        self.seq = (self.seq + 1) % (1 << 31)
        return self.seq

    def _make_packet(self, msgtype: str, body: str) -> str:
        """Build packet string according to format spec."""
        seq = self._next_seq()
        header = f"{msgtype}|{seq}|{self.id}|{self.ip}|{self.port}"
        return f"{header}|{body}"

    def _send(self, data: str, addr: tuple):
        """Send encoded UDP packet to destination."""
        if not self.sock:
            return
        try:
            self.sock.sendto(data.encode("utf-8"), addr)
        except OSError as e:
            self._log("ERROR", f"send failed to {addr}: {e}")

    # setting up message handlers
    def broadcast_sync(self):
        """Broadcast [PEER_SYNC] message to announce presence."""
        body = json.dumps({"ts": time.time()})
        pkt = self._make_packet("PEER_SYNC", body)
        self._send(pkt, (BROADCAST_IP, self.port))
        self._log("SYNC", f"Broadcasted presence on {BROADCAST_IP}:{self.port}")

    def send_ping(self, peer_id: str, peer_info: dict):
        """Send [PING] message to a specific peer."""
        ts = time.time()
        body = json.dumps({"ts": ts})
        pkt = self._make_packet("PING", body)
        self._send(pkt, (peer_info["ip"], int(peer_info["port"])) )
        self._last_ping[peer_id] = ts
        self._pending[(peer_id, self.seq)] = ts 
        self._log("PING", f"-> {peer_id} ({peer_info['ip']}:{peer_info['port']}) seq={self.seq}")

    def send_pong(self, addr: tuple):
        """Send [PONG] message back to sender."""
        body = json.dumps({"ts": time.time()})
        pkt = self._make_packet("PONG", body)
        self._send(pkt, addr)

    def handle_message(self, msg: str, addr: tuple):
        """Parse and process incoming UDP packet."""
        parts = msg.split("|", 5)
        if len(parts) != 6:
            return
        mtype, seq_s, from_id, from_ip, from_port, body = parts
        if from_id == self.id:
            return

        # ensure peer is in table, update last_seen
        now = time.time()
        with self.lock:
            p = self.peers.get(from_id)
            if not p:
                self.peers[from_id] = {
                    "ip": from_ip,
                    "port": int(from_port),
                    "last_seen": now,
                    "status": "active",
                    "rtt_ms": None,
                }
                self._log("SYNC", f"Added {from_id} ({from_ip})")
            else:
                # update IP/port if changed. bump last_seen
                changed = (p["ip"], p["port"]) != (from_ip, int(from_port))
                p["ip"], p["port"], p["last_seen"], p["status"] = from_ip, int(from_port), now, "active"
                if changed:
                    self._log("INFO", f"Peer {from_id} moved to {from_ip}:{from_port}")

        # handle types
        try:
            payload = json.loads(body) if body else {}
        except json.JSONDecodeError:
            payload = {}
        seq = int(seq_s)

        if mtype == "PEER_SYNC":
            # nothing else to do, listener already updated table
            return
        elif mtype == "PING":
            # reply with PONG
            self.send_pong(addr)
            return
        elif mtype == "PONG":
            # compute RTT using the timestamp the peer included
            sent_ts = payload.get("ts")
            if sent_ts is not None:
                rtt_ms = (now - float(sent_ts)) * 1000.0
                with self.lock:
                    peer = self.peers.get(from_id)
                    if peer:
                        # update RTT and last_seen
                        peer["rtt_ms"] = rtt_ms
                        peer["last_seen"] = now
                self._log("PONG", f"<- {from_id} rtt≈{rtt_ms:.1f} ms")
            return
        else:
            self._log("WARN", f"Unknown type {mtype} from {from_id}")

    # setting up threads
    def listener(self):
        """Continuously listen for incoming packets."""
        while self.running:
            try:
                data, addr = self.sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                msg = data.decode("utf-8", errors="ignore")
            except Exception:
                continue
            self.handle_message(msg, addr)

    def broadcaster(self):
        """Periodically broadcast PEER_SYNC messages."""
        while self.running:
            self.broadcast_sync()
            self._write_snapshot()
            time.sleep(SYNC_INTERVAL)

    def heartbeat(self):
        """Send pings and remove inactive peers."""
        while self.running:
            now = time.time()
            to_remove = []
            with self.lock:
                items = list(self.peers.items())
            for peer_id, info in items:
                # remove stale peers
                if now - info.get("last_seen", 0) > REMOVE_TIMEOUT:
                    to_remove.append(peer_id)
                    continue
                # ping schedule
                last = self._last_ping.get(peer_id, 0)
                if now - last >= PING_INTERVAL:
                    self.send_ping(peer_id, info)
            if to_remove:
                with self.lock:
                    for pid in to_remove:
                        self.peers.pop(pid, None)
                        self._last_ping.pop(pid, None)
                        self._log("INFO", f"Removed stale peer {pid}")
            time.sleep(1)

    def summary(self):
        """Print peer-table summary periodically."""
        while self.running:
            with self.lock:
                peers_copy = {k: dict(v) for k, v in self.peers.items()}
            lines = [f"Peers ({len(peers_copy)}):"]
            for pid, info in sorted(peers_copy.items()):
                rtt = info.get("rtt_ms")
                rtt_str = f"{rtt:.1f} ms" if rtt is not None else "n/a"
                lines.append(f"  - {pid} @ {info['ip']}:{info['port']} | last_seen={int(time.time()-info['last_seen'])}s | rtt={rtt_str}")
            self._log("INFO", "\n".join(lines))
            # snapshot also handled by broadcaster, this keeps it fresh if no broadcasts
            self._write_snapshot()
            time.sleep(10)

    # node control
    def start(self):
        """Start listener, broadcaster, heartbeat, and summary threads."""
        if self.running:
            return
        self.ip = self._get_local_ip()
        self._setup_socket()
        self._log_path = os.path.join("logs", f"{self.id}.log")
        self._snapshot_path = os.path.join("logs", f"{self.id}_peers_snapshot.txt")
        self.running = True
        self._log("INFO", f"Node {self.id} started at {self.ip}:{self.port}")
        t1 = threading.Thread(target=self.listener, daemon=True)
        t2 = threading.Thread(target=self.broadcaster, daemon=True)
        t3 = threading.Thread(target=self.heartbeat, daemon=True)
        t4 = threading.Thread(target=self.summary, daemon=True)
        self._threads = [t1, t2, t3, t4]
        for t in self._threads:
            t.start()

    def stop(self):
        """Stop all threads and close socket."""
        self.running = False
        try:
            if self.sock:
                self.sock.close()
        except OSError:
            pass
        self._log("EXIT", "Node stopping")

    # helpers
    def _write_snapshot(self):
        if not self._snapshot_path:
            return
        with self.lock:
            snapshot = {
                pid: {
                    "ip": info.get("ip"),
                    "port": info.get("port"),
                    "last_seen": int(info.get("last_seen", 0)),
                    "status": info.get("status", "active"),
                    "rtt_ms": None if info.get("rtt_ms") is None else round(info["rtt_ms"], 1),
                }
                for pid, info in self.peers.items()
            }
        try:
            with open(self._snapshot_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(snapshot, indent=2))
        except OSError:
            pass

    def _log(self, level: str, message: str):
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        line = f"[{level}] | {ts} | {message}"
        print(line, flush=True)
        try:
            if self._log_path:
                with open(self._log_path, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
        except OSError:
            pass


# main entry point
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python node_interface.py <node_id>")
        exit(1)

    node_id = sys.argv[1]
    node = PeerNode(node_id)
    node.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        node.stop()
        print("\n[EXIT] Node stopped.")
