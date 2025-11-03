
#!/usr/bin/env python3
"""
UDP Overlay Networking Implementation
-------------------------------------
Implements discovery (PEER_SYNC), heartbeat (PING/PONG), table maintenance,
and simple metrics (reliability and RTT) following the provided README spec.
"""

import socket
import threading
import time
import zlib
from typing import Tuple, Dict, Any

import logging
import os

def _setup_logging(node_id: str):
    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger(node_id)
    logger.setLevel(logging.INFO)

    # File handler per node
    fh = logging.FileHandler(f"logs/{node_id}.log", encoding="utf-8")
    fh.setLevel(logging.INFO)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)

    logger.handlers.clear()
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


# ---------- Global Configuration ----------
PORT = 5000
BROADCAST_IP = "192.168.0.255"
SYNC_INTERVAL = 5          # seconds
PING_INTERVAL = 15         # seconds
PING_TIMEOUT = 3           # seconds
REMOVE_TIMEOUT = 30        # seconds
DEFAULT_TTL = 3
MODEL_VER = 0

def _now_ms() -> int:
    return int(time.time() * 1000)

def _crc32_hex(s: str) -> str:
    return f"{zlib.crc32(s.encode('utf-8')) & 0xFFFFFFFF:08x}"


class PeerNode:
    """
    Represents a single node in the decentralized chatbot overlay.
    Implements UDP broadcast discovery and heartbeat logic.
    """

    def __init__(self, node_id: str):
        """Initialize node state, socket, and peer table."""
        self.id = node_id
        self.logger = _setup_logging(node_id)
        self.ip = None
        self.port = PORT
        self.seq = 0
        self.sock: socket.socket | None = None
        # peers: {peer_id: {"ip":..., "port":..., "last_seen":epoch_s, "status":"active"}}
        self.peers: Dict[str, Dict[str, Any]] = {}
        self.running = False
        self.lock = threading.Lock()

        # metrics
        self.sent = 0
        self.recv = 0
        self.rtts_ms = []          # list of RTTs in ms
        # ping_out: {peer_id: {"t_ms":..., "addr":(ip,port)}}
        self.ping_out: Dict[str, Dict[str, Any]] = {}

        # threads
        self._threads: list[threading.Thread] = []

        # init
        self._setup_socket()
        self.ip = self._get_local_ip()
        self.logger.info(f"INIT Node {self.id} at {self.ip}:{self.port}")

    # ---------- Setup ----------
    def _setup_socket(self):
        """Create and configure UDP socket for broadcast and unicast."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        # Allow rebinding on restarts
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(('', PORT))
        except OSError as e:
            # Provide clearer message
            self.logger.error(f"Could not bind UDP port {PORT}: {e}")
            raise SystemExit(1)
        # Non-blocking via timeout recv
        sock.settimeout(0.2)
        self.sock = sock

    def _get_local_ip(self):
        """Return the local IP address of this host."""
        ip = "127.0.0.1"
        try:
            probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            # Doesn't actually send packets to the internet, but lets the OS pick a route
            probe.connect(("8.8.8.8", 80))
            ip = probe.getsockname()[0]
            probe.close()
        except Exception:
            # Fallbacks
            try:
                ip = socket.gethostbyname(socket.gethostname())
            except Exception:
                pass
        return ip

    # ---------- Message Handling ----------
    def _next_seq(self) -> int:
        """Increment and return the next sequence number."""
        self.seq += 1
        return self.seq

    def _make_packet(self, msgtype: str, body: str) -> str:
        """Build packet string according to format spec.

        Format:
            [MSGTYPE]|<SENDER_ID>|<SEQ_ID>|<TIMESTAMP>|<MODEL_VER>|<TTL>|<BODY>|<CRC32>
        CRC32 is computed over everything up to but NOT including the final '|<CRC32>'.
        """
        header = f"[{msgtype}]|{self.id}|{self._next_seq()}|{_now_ms()}|{MODEL_VER}|{DEFAULT_TTL}"
        no_crc = f"{header}|{body}"
        crc = _crc32_hex(no_crc)
        return f"{no_crc}|{crc}"

    def _send(self, data: str, addr: Tuple[str, int]):
        """Send encoded UDP packet to destination."""
        if not self.sock:
            return
        try:
            self.sock.sendto(data.encode('utf-8'), addr)
            self.sent += 1
        except Exception as e:
            self.logger.error(f"SEND-ERR to {addr}: {e}")

    # ---------- Overlay Operations ----------
    def broadcast_sync(self):
        """Broadcast [PEER_SYNC] message to announce presence."""
        body = f"{self.ip},{self.port}"
        pkt = self._make_packet("PEER_SYNC", body)
        self._send(pkt, (BROADCAST_IP, PORT))

    def send_ping(self, peer_id: str, peer_info: dict):
        """Send [PING] message to a specific peer."""
        target = (peer_info["ip"], peer_info["port"])
        body = f"{self.ip},{_now_ms()}"
        pkt = self._make_packet("PING", body)
        self.ping_out[peer_id] = {"t_ms": _now_ms(), "addr": target}
        self._send(pkt, target)

    def send_pong(self, addr: Tuple[str, int], echo_ts: int):
        """Send [PONG] message back to sender. Body: '<IP_sender>,ok,<rtt>' (rtt filled by receiver)."""
        # For PONG, we can include our IP and echo back "ok,0" (RTT will be measured by the pinger)
        body = f"{self.ip},ok,0"
        pkt = self._make_packet("PONG", body)
        self._send(pkt, addr)

    def _parse_packet(self, msg: str) -> Tuple[str, str, int, int, int, int, str, str]:
        """
        Returns tuple:
          (msgtype, sender_id, seq_id, ts_ms, model_ver, ttl, body, crc_hex)
        Raises ValueError on parse or CRC errors.
        """
        parts = msg.split('|')
        if len(parts) < 8:
            raise ValueError("too few fields")

        # parts[0] like "[PEER_SYNC]"
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

        # verify CRC
        without_crc = '|'.join(parts[:7])
        calc = _crc32_hex(without_crc)
        if calc != crc_hex:
            raise ValueError("crc mismatch")
        return msgtype, sender_id, seq_id, ts_ms, model_ver, ttl, body, crc_hex

    def handle_message(self, msg: str, addr: Tuple[str, int]):
        """Parse and process incoming UDP packet."""
        try:
            parsed = self._parse_packet(msg)
        except Exception as e:
            # drop silently or log
            # print(f"[DROP] malformed from {addr}: {e}")
            return

        msgtype, sender_id, seq_id, ts_ms, model_ver, ttl, body, crc_hex = parsed

        # self-filter
        if sender_id == self.id:
            return

        # count received
        self.recv += 1

        # keep table updated on any message
        now_s = int(time.time())
        with self.lock:
            # ensure peer in table (ip from packet body for sync; else from addr)
            if sender_id not in self.peers:
                # resolve IP/port
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
                self.logger.info(f"SYNC Added {sender_id} ({self.peers[sender_id]['ip']})")
            else:
                self.peers[sender_id]["last_seen"] = now_s
                self.peers[sender_id]["status"] = "active"

        # message-specific handling
        if msgtype == "PEER_SYNC":
            # nothing else needed
            return
        elif msgtype == "PING":
            # try to parse body "<IP_sender>,<timestamp>"
            try:
                ip_sender, ts_str = body.split(',')
                echo_ts = int(ts_str)
            except Exception:
                echo_ts = _now_ms()
            self.send_pong(addr, echo_ts)
        elif msgtype == "PONG":
            # PING counterpart: compute RTT for this peer if outstanding
            # body: "<IP_sender>,ok,<rtt>" (rtt ignored; compute locally)
            with self.lock:
                if sender_id in self.ping_out:
                    t0 = self.ping_out[sender_id]["t_ms"]
                    rtt = _now_ms() - t0
                    self.rtts_ms.append(rtt)
                    # print RTT
                    self.logger.info(f"PING RTT={rtt}ms to {sender_id}")
                    del self.ping_out[sender_id]
        else:
            # ignore unknown types for now
            pass

    # ---------- Thread Tasks ----------
    def listener(self):
        """Continuously listen for incoming packets."""
        assert self.sock is not None
        while self.running:
            try:
                data, addr = self.sock.recvfrom(8192)
            except socket.timeout:
                continue
            except OSError:
                # socket closed
                break
            try:
                msg = data.decode('utf-8', errors='ignore')
            except Exception:
                continue
            self.handle_message(msg, addr)

    def broadcaster(self):
        """Periodically broadcast PEER_SYNC messages."""
        next_time = time.time()
        while self.running:
            now = time.time()
            if now >= next_time:
                self.broadcast_sync()
                next_time = now + SYNC_INTERVAL
            time.sleep(0.1)

    def heartbeat(self):
        """Send pings and remove inactive peers."""
        last_ping = 0.0
        while self.running:
            now = time.time()
            # send pings every PING_INTERVAL
            if now - last_ping >= PING_INTERVAL:
                with self.lock:
                    # snapshot peers to avoid holding lock while sending
                    peers_snapshot = {pid: dict(info) for pid, info in self.peers.items()}
                for pid, info in peers_snapshot.items():
                    self.send_ping(pid, info)
                last_ping = now

            # check timeouts for outstanding pings
            with self.lock:
                to_remove_out = []
                for pid, rec in self.ping_out.items():
                    if (_now_ms() - rec["t_ms"]) / 1000.0 > PING_TIMEOUT:
                        # timeout: keep for reliability metric; just drop outstanding
                        to_remove_out.append(pid)
                for pid in to_remove_out:
                    # print timeout info but do not remove from peer table here;
                    # removal happens via last_seen threshold below.
                    self.logger.info(f"TIMEOUT No PONG from {pid} within {PING_TIMEOUT}s")
                    del self.ping_out[pid]

            # remove peers inactive for REMOVE_TIMEOUT
            cutoff = int(time.time()) - REMOVE_TIMEOUT
            with self.lock:
                dead = [pid for pid, info in self.peers.items() if info["last_seen"] < cutoff]
                for pid in dead:
                    self.logger.info(f"DROP {pid} removed (timeout)")
                    del self.peers[p] if False else None  # placeholder to keep syntax branchers happy
                for pid in dead:
                    self.peers.pop(pid, None)

            time.sleep(0.2)

    def summary(self):
        """Print peer-table summary periodically."""
        while self.running:
            with self.lock:
                n = len(self.peers)
                reliability = (self.recv / self.sent) if self.sent > 0 else 0.0
                mean_rtt = (sum(self.rtts_ms) / len(self.rtts_ms)) if self.rtts_ms else 0.0
            self.logger.info(f"TABLE {n} active | R={reliability:.2f} | mean RTT={mean_rtt:.1f}ms")
            time.sleep(5)

    # ---------- Control ----------
    def start(self):
        """Start listener, broadcaster, heartbeat, and summary threads."""
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
        """Stop all threads and close socket."""
        if not self.running:
            return
        self.running = False
        # allow loops to exit
        time.sleep(0.3)
        try:
            if self.sock:
                self.sock.close()
        except Exception:
            pass
        # join non-daemon threads (these are daemons; but try gentle wait)
        for t in self._threads:
            if t.is_alive():
                t.join(timeout=0.5)
        self.logger.info("STOP Node stopped cleanly")


# ---------- Entry Point ----------
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
