
#!/usr/bin/env python3
import socket
import threading
import time
import zlib
import logging
import os
import json
from typing import Tuple, Dict, Any

# ---------- Global Configuration ----------
PORT = 5000
BROADCAST_IP = "192.168.0.255"  # change to your subnet's broadcast if needed
SYNC_INTERVAL = 5          # seconds
PING_INTERVAL = 15         # seconds
PING_TIMEOUT = 3           # seconds
REMOVE_TIMEOUT = 30        # seconds
DEFAULT_TTL = 3
MODEL_VER = 0

def _setup_logging(node_id: str):
    os.makedirs("logs", exist_ok=True)

    # Main logger (events)
    logger = logging.getLogger(node_id)
    logger.setLevel(logging.INFO)

    fh = logging.FileHandler(f"logs/{node_id}.log", encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] | %(message)s"))  # file
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("[%(levelname)s] | %(message)s"))  # console

    logger.handlers.clear()
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger

def _now_ms() -> int:
    return int(time.time() * 1000)

def _crc32_hex(s: str) -> str:
    return f"{zlib.crc32(s.encode('utf-8')) & 0xFFFFFFFF:08x}"


class PeerNode:
    def __init__(self, node_id: str):
        self.id = node_id
        self.logger = _setup_logging(node_id)
        self.ip = None
        self.port = PORT
        self.seq = 0
        self.sock: socket.socket | None = None
        self.peers: Dict[str, Dict[str, Any]] = {}
        self.running = False
        self.lock = threading.Lock()

        self.sent = 0
        self.recv = 0
        self.rtts_ms = []
        self.ping_out: Dict[str, Dict[str, Any]] = {}

        self._threads: list[threading.Thread] = []

        self._setup_socket()
        self.ip = self._get_local_ip()
        self.logger.info(f"INIT Node {self.id} at {self.ip}:{self.port}")

    # ---------- Setup ----------
    def _setup_socket(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(('', PORT))
        except OSError as e:
            self.logger.error(f"Could not bind UDP port {PORT}: {e}")
            raise SystemExit(1)
        sock.settimeout(0.2)
        self.sock = sock

    def _get_local_ip(self):
        ip = "127.0.0.1"
        try:
            probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            probe.connect(("8.8.8.8", 80))
            ip = probe.getsockname()[0]
            probe.close()
        except Exception:
            try:
                ip = socket.gethostbyname(socket.gethostname())
            except Exception:
                pass
        return ip

    # ---------- Message Handling ----------
    def _next_seq(self) -> int:
        self.seq += 1
        return self.seq

    def _make_packet(self, msgtype: str, body: str) -> str:
        header = f"[{msgtype}]|{self.id}|{self._next_seq()}|{_now_ms()}|{MODEL_VER}|{DEFAULT_TTL}"
        no_crc = f"{header}|{body}"
        crc = _crc32_hex(no_crc)
        return f"{no_crc}|{crc}"

    def _send(self, data: str, addr: Tuple[str, int]):
        if not self.sock:
            return
        try:
            self.sock.sendto(data.encode('utf-8'), addr)
            self.sent += 1
        except Exception as e:
            self.logger.error(f"SEND-ERR to {addr}: {e}")

    # ---------- Overlay Operations ----------
    def broadcast_sync(self):
        body = f"{self.ip},{self.port}"
        pkt = self._make_packet("PEER_SYNC", body)
        self._send(pkt, (BROADCAST_IP, PORT))

    def send_ping(self, peer_id: str, peer_info: dict):
        target = (peer_info["ip"], peer_info["port"])
        body = f"{self.ip},{_now_ms()}"
        pkt = self._make_packet("PING", body)
        self.ping_out[peer_id] = {"t_ms": _now_ms(), "addr": target}
        self._send(pkt, target)

    def send_pong(self, addr: Tuple[str, int], echo_ts: int):
        body = f"{self.ip},ok,0"
        pkt = self._make_packet("PONG", body)
        self._send(pkt, addr)

    def _parse_packet(self, msg: str):
        parts = msg.split('|')
        if len(parts) < 8:
            raise ValueError("too few fields")
        if not (parts[0].startswith('[') and parts[0].endswith(']')):
            raise ValueError("bad type bracket")
        msgtype = parts[0][1:-1]
        sender_id = parts[1]
        seq_id = int(parts[2])
        ts_ms = int(parts[3])
        model_ver = int(parts[4])
        ttl = int(parts[5])
        body = parts[6]
        crc_hex = parts[7]
        without_crc = '|'.join(parts[:7])
        calc = _crc32_hex(without_crc)
        if calc != crc_hex:
            raise ValueError("crc mismatch")
        return msgtype, sender_id, seq_id, ts_ms, model_ver, ttl, body, crc_hex

    def handle_message(self, msg: str, addr: Tuple[str, int]):
        try:
            parsed = self._parse_packet(msg)
        except Exception:
            return

        msgtype, sender_id, seq_id, ts_ms, model_ver, ttl, body, crc_hex = parsed
        if sender_id == self.id:
            return
        self.recv += 1

        now_s = int(time.time())
        with self.lock:
            if sender_id not in self.peers:
                ip_port = addr
                if msgtype == "PEER_SYNC":
                    try:
                        ip_str, port_str = body.split(',')
                        ip_port = (ip_str, int(port_str))
                    except Exception:
                        ip_port = addr
                self.peers[sender_id] = {
                    "ip": ip_port[0],
                    "port": ip_port[1],
                    "last_seen": now_s,
                    "status": "active",
                }
                self.logger.info(f"[SYNC] Added {sender_id} ({self.peers[sender_id]['ip']})")
            else:
                self.peers[sender_id]["last_seen"] = now_s
                self.peers[sender_id]["status"] = "active"

        if msgtype == "PEER_SYNC":
            return
        elif msgtype == "PING":
            try:
                ip_sender, ts_str = body.split(',')
                echo_ts = int(ts_str)
            except Exception:
                echo_ts = _now_ms()
            self.send_pong(addr, echo_ts)
        elif msgtype == "PONG":
            with self.lock:
                if sender_id in self.ping_out:
                    t0 = self.ping_out[sender_id]["t_ms"]
                    rtt = _now_ms() - t0
                    self.rtts_ms.append(rtt)
                    self.logger.info(f"[PING] RTT={rtt}ms to {sender_id}")
                    del self.ping_out[sender_id]

    # ---------- Peer table dumps ----------
    def dump_peers_sample_style(self):
        with self.lock:
            obj = self.peers
            txt = 'peers = ' + json.dumps(obj, indent=3, separators=(',',':'))
        return txt

    def write_peers_snapshot(self, path=None):
        if path is None:
            path = f"logs/peers_snapshot_{self.id}.txt"
        with self.lock:
            obj = self.peers
            txt = 'peers = ' + json.dumps(obj, indent=3, separators=(',',':'))
        with open(path, "w", encoding="utf-8") as f:
            f.write(txt + "\n")

    # ---------- Thread Tasks ----------
    def listener(self):
        assert self.sock is not None
        while self.running:
            try:
                data, addr = self.sock.recvfrom(8192)
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                msg = data.decode('utf-8', errors='ignore')
            except Exception:
                continue
            self.handle_message(msg, addr)

    def broadcaster(self):
        next_time = time.time()
        while self.running:
            now = time.time()
            if now >= next_time:
                self.broadcast_sync()
                next_time = now + SYNC_INTERVAL
            time.sleep(0.1)

    def heartbeat(self):
        last_ping = 0.0
        while self.running:
            now = time.time()
            if now - last_ping >= PING_INTERVAL:
                with self.lock:
                    peers_snapshot = {pid: dict(info) for pid, info in self.peers.items()}
                for pid, info in peers_snapshot.items():
                    self.send_ping(pid, info)
                last_ping = now

            with self.lock:
                to_remove_out = []
                for pid, rec in self.ping_out.items():
                    if (_now_ms() - rec["t_ms"]) / 1000.0 > PING_TIMEOUT:
                        to_remove_out.append(pid)
                for pid in to_remove_out:
                    self.logger.info(f"[TIMEOUT] No PONG from {pid} within {PING_TIMEOUT}s")
                    del self.ping_out[pid]

            cutoff = int(time.time()) - REMOVE_TIMEOUT
            with self.lock:
                dead = [pid for pid, info in self.peers.items() if info["last_seen"] < cutoff]
                for pid in dead:
                    self.logger.info(f"[DROP] {pid} removed (timeout)")
                    self.peers.pop(pid, None)

            time.sleep(0.2)

    def summary(self):
        while self.running:
            with self.lock:
                n = len(self.peers)
            self.logger.info(f"[TABLE] {n} active peers")
            pass  # peer table not logged; snapshot written on stop
            time.sleep(10)

    # ---------- Control ----------
    def start(self):
        if self.running:
            return
        self.running = True
        t_listener = threading.Thread(target=self.listener, name="listener", daemon=True)
        t_bcast = threading.Thread(target=self.broadcaster, name="broadcaster", daemon=True)
        t_hb = threading.Thread(target=self.heartbeat, name="heartbeat", daemon=True)
        t_sum = threading.Thread(target=self.summary, name="summary", daemon=True)
        self._threads = [t_listener, t_bcast, t_hb, t_sum]
        for t in self._threads:
            t.start()
        self.logger.info("START Threads: listener, broadcaster, heartbeat, summary")

    def stop(self):
        if not self.running:
            return
        self.running = False
        time.sleep(0.3)
        try:
            if self.sock:
                self.sock.close()
        except Exception:
            pass
        for t in self._threads:
            if t.is_alive():
                t.join(timeout=0.5)
        # Write a final snapshot file on exit
        self.write_peers_snapshot()
        self.logger.info("STOP Node stopped cleanly")


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python node.py <node_id>")
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
