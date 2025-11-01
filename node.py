import socket
import threading
import time
import logging
import os
from utils import calculate_crc32, parse_packet, current_millis

# -----------------------------
# Logging setup
# -----------------------------
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    filename='logs/node.log',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('[%(levelname)s] %(message)s')
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

# -----------------------------
# Configuration
# -----------------------------
UDP_PORT = 5000
BROADCAST_INTERVAL = 5      # seconds
HEARTBEAT_INTERVAL = 15     # seconds
PING_TIMEOUT = 3            # seconds
PEER_TIMEOUT = 30           # seconds
NODE_ID = "Pi-1"            # change for each node

peers = {}  # peer table

seq_id = 0

# -----------------------------
# Socket setup
# -----------------------------
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
sock.settimeout(0.5)
sock.bind(("", UDP_PORT))

# -----------------------------
# Broadcast PEER_SYNC
# -----------------------------
def broadcast_loop():
    global seq_id
    while True:
        seq_id += 1
        timestamp = current_millis()
        local_ip = sock.getsockname()[0]
        body = f"{local_ip},{UDP_PORT}"
        header = f"PEER_SYNC|{NODE_ID}|{seq_id}|{timestamp}|0|3"
        crc = calculate_crc32(f"{header}|{body}")
        packet = f"{header}|{body}|{crc}"
        sock.sendto(packet.encode(), ("<broadcast>", UDP_PORT))
        time.sleep(BROADCAST_INTERVAL)

# -----------------------------
# Receive loop
# -----------------------------
def receive_loop():
    while True:
        try:
            data, addr = sock.recvfrom(1024)
            packet = data.decode()
            parsed = parse_packet(packet)
            if not parsed.get("valid"):
                logging.error(f"[ERROR] Invalid packet from {addr}")
                continue
            header = parsed["header"]
            body = parsed["body"]
            sender = header["SENDER_ID"]

            if sender == NODE_ID:
                continue  # ignore self

            # Handle PEER_SYNC
            if header["MSGTYPE"] == "PEER_SYNC":
                ip, port = body.split(",")
                port = int(port)
                if sender not in peers:
                    peers[sender] = {"ip": ip, "port": port, "last_seen": time.time(), "status": "active"}
                    logging.info(f"[SYNC] Added {sender} ({ip})")
                else:
                    peers[sender]["last_seen"] = time.time()
                    logging.info(f"[SYNC] Refreshed {sender} ({ip})")

            # Handle PING
            elif header["MSGTYPE"] == "PING":
                ip_sender, ts = body.split(",")
                ts = int(ts)
                rtt = current_millis() - ts
                pong_body = f"{sock.getsockname()[0]},ok,{rtt}"
                pong_header = f"PONG|{NODE_ID}|{seq_id}|{current_millis()}|0|3"
                crc = calculate_crc32(f"{pong_header}|{pong_body}")
                pong_packet = f"{pong_header}|{pong_body}|{crc}"
                sock.sendto(pong_packet.encode(), (ip_sender, UDP_PORT))

            # Handle PONG
            elif header["MSGTYPE"] == "PONG":
                ip_sender, status, rtt = body.split(",")
                logging.info(f"[PONG] RTT={rtt}ms from {sender}")

        except socket.timeout:
            continue
        except Exception as e:
            logging.error(f"[ERROR] {e}")

# -----------------------------
# Heartbeat loop
# -----------------------------
def heartbeat_loop():
    while True:
        for peer_id, peer in list(peers.items()):
            try:
                ts = current_millis()
                body = f"{sock.getsockname()[0]},{ts}"
                header = f"PING|{NODE_ID}|{seq_id}|{ts}|0|3"
                crc = calculate_crc32(f"{header}|{body}")
                packet = f"{header}|{body}|{crc}"
                sock.sendto(packet.encode(), (peer["ip"], UDP_PORT))
                logging.info(f"[PING] Sent to {peer_id} ({peer['ip']})")
            except Exception as e:
                logging.error(f"[ERROR] {e}")
        # Remove peers inactive for > PEER_TIMEOUT
        now = time.time()
        for peer_id in list(peers.keys()):
            if now - peers[peer_id]["last_seen"] > PEER_TIMEOUT:
                logging.info(f"[DROP] {peer_id} removed (timeout)")
                del peers[peer_id]

        # Print table summary
        logging.info(f"[TABLE] {len(peers)} active peers: {', '.join(peers.keys())}")
        time.sleep(HEARTBEAT_INTERVAL)

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    threading.Thread(target=broadcast_loop, daemon=True).start()
    threading.Thread(target=receive_loop, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Shutting down node...")
        sock.close()
