# Raspberry-Pi-Project
Raspberry Pi group project for CSC 4200

Group Members: Will Ward, Nick Atkins, Liam Owenby

### INSTRUCTIONS

### Project Context

This course-long project builds a decentralized LLM agent running across Raspberry Pis.
Each Pi hosts a local lightweight language model that learns from user interactions and synchronizes new knowledge with peers using UDP.

This first assignment establishes the networking layer — the foundation for distributed learning, trust computation, and model exchange in later assignments.

---

## 1. System Overview

Each node runs four coordinated subsystems:

```text
+--------------------------------------------------------------+
|                     Application Layer                        |
|  - Decentralized Chatbot Interface                           |
|  - Local Q/A Memory and Fine-Tuning Scheduler                 |
|  - Trust & Reputation Computation (T, R, D, ρ)               |
+--------------------------------------------------------------+
|                Learning / Model Management Layer              |
|  - Lightweight LLM (TinyLlama / DistilGPT-2)                 |
|  - Incremental Update + Merge Logic                          |
|  - Validation Before Accepting Peer Updates                  |
+--------------------------------------------------------------+
|                Overlay Networking Layer (UDP)                 |
|  - Peer Discovery (Broadcast PEER_SYNC)                       |
|  - Peer Table Synchronization (Unicast PING/PONG)            |
|  - Reliability (ACK/NACK, Retransmission, Ordering)          |
|  - Message Types: SYNC, PING, PONG, UPDATE, ALERT            |
+--------------------------------------------------------------+
|                 Transport / Physical Layer                    |
|  - UDP Datagram Sockets over LAN/Wi-Fi                       |
|  - Traffic Shaping for Fault Injection (tc)                  |
+--------------------------------------------------------------+
```

In this assignment, students implement only the Overlay Networking Layer.

---

## 2. Assignment 1 — Networking & Overlay Foundation

### Objective

Students will implement a UDP-based discovery and synchronization system where all Raspberry Pis on the same subnet automatically discover each other, maintain an updated peer table, and confirm liveness via heartbeat messages.

This layer must follow the packet formats specified below.

---

### 2.1 Learning Outcomes

After completing this assignment, students will:

1. Implement UDP broadcast and unicast messaging.
2. Build and maintain a synchronized peer table.
3. Follow strict packet and header specifications.
4. Measure reliability and latency.
5. Produce clean logs and a short team report.

---

### 2.2 Implementation Tasks

#### 1. Socket Initialization

* All nodes use the same UDP port (`5000`).
* Enable broadcast:

  ```python
  sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
  ```
* Non-blocking receive loop with timeout.

#### 2. Peer Discovery

* Every 5 s, each node broadcasts:

  ```
  [PEER_SYNC]|<id>|<seq>|<timestamp>|0|3|<ip>,<port>|<crc>
  ```
* On receipt, add or refresh the peer in the table.
* Ignore self-messages.

#### 3. Heartbeat and Table Maintenance

* Every 15 s, send `[PING]` to all peers.
* Expect `[PONG]` within 3 s.
* Remove inactive peers after 30 s.
* Maintain table structure:

  ```python
  peers = {
     "Pi-1": {"ip":"192.168.0.10","port":5000,"last_seen":1729720200,"status":"active"},
     ...
  }
  ```

#### 4. Logging and Metrics

Log discovery, ping replies, timeouts, and table size:

```
[SYNC] Added Pi-3 (192.168.0.13)
[PING] RTT=21ms to Pi-2
[DROP] Pi-1 removed (timeout)
[TABLE] 3 active peers
```

Compute:

* Reliability `R = recv/sent`
* Mean RTT (ms)

#### 5. Verification

Run 3–4 nodes on one subnet.
All nodes must display the same peer table after 20 s.

---

### 2.3 Deliverables

1. Source Code:

   * `node.py` implementing broadcast, unicast, and heartbeat.
2. Peer Table Snapshot: identical across nodes.
3. Log File: showing discovery and ping/pong events.
4. Short Report (≤2 pages) using provided template.
5. Demo: 2-minute in-lab test showing synchronized overlay.

---

### 2.4 Grading Rubric (30 points)

| Category               | Criteria                                              | Points |
| ---------------------- | ----------------------------------------------------- | ------ |
| Broadcast Discovery    | Nodes auto-discover within 5 s; packet format correct | 8      |
| Unicast Heartbeat      | Proper PING/PONG logic and timeout handling           | 8      |
| Peer Table Consistency | All nodes maintain synchronized tables                | 6      |
| Metrics and Logging    | Reliability and RTT printed or logged                 | 4      |
| Report Quality         | Correct structure and contribution table              | 4      |
| Bonus              | CRC32 integrity check implemented                     | +2     |

---

## 3. UDP Packet Format Specification

All messages must follow this structure:

```
[HEADER]|[BODY]|[CRC32]
```

### Header Fields

| Field     | Description                     | Example                     |
| --------- | ------------------------------- | --------------------------- |
| MSGTYPE   | Message type                    | `PEER_SYNC`, `PING`, `PONG` |
| SENDER_ID | Unique string ID                | `Pi-2`                      |
| SEQ_ID    | Sequence number                 | `102`                       |
| TIMESTAMP | UNIX epoch in ms                | `1729720830000`             |
| MODEL_VER | Always `0` (reserved for later) | `0`                         |
| TTL       | Hop counter (default 3)         | `3`                         |

### Body Formats

| MSGTYPE     | Direction | Body Format               | Purpose           |
| ----------- | --------- | ------------------------- | ----------------- |
| `PEER_SYNC` | broadcast | `<IP>,<PORT>`             | Announce presence |
| `PING`      | unicast   | `<IP_sender>,<timestamp>` | Check liveness    |
| `PONG`      | unicast   | `<IP_sender>,ok,<rtt>`    | Respond to ping   |

### CRC32 Field

* Calculated over `[HEADER]|[BODY]`.
* Represented as 8-char hexadecimal string.
* Invalid CRC → discard packet.

Example full packet:

```
[PEER_SYNC]|Pi-1|1001|1729720815000|0|3|192.168.0.10,5000|d2b8e0ac
```

---

## 4. Report Template

### Team Information

| Name     | Role / Contribution            | Files          | Hours |
| -------- | ------------------------------ | -------------- | ----- |
| Alice K. | Broadcast + message parser     | `node.py`      | 7     |
| Ben L.   | Heartbeat and peer-table logic | `heartbeat.py` | 6     |
| Chen R.  | Logging, RTT analysis, report  | `metrics.py`   | 5     |

### 1. System Setup

* Hardware: Raspberry Pi 4 × 3
* Network: Wi-Fi 192.168.0.0/24
* UDP Port: 5000

### 2. Protocol Summary

* Broadcast: 5 s
* Ping: 15 s
* Timeout: 3 s
* Removal: 30 s

### 3. Results

| Metric         | Value   | Notes                 |
| -------------- | ------- | --------------------- |
| Avg RTT        | 21.4 ms | from PING/PONG        |
| Packet loss    | 3.1 %   | via tcpdump           |
| Discovery time | 8.5 s   | until full table sync |

### 4. Logs

```
[SYNC] Pi-2 joined (192.168.0.12)
[SYNC] Pi-3 joined (192.168.0.13)
[PING] RTT=19ms | [PONG] OK
```

### 5. Discussion

* Discovery stable under <5% loss.
* Some duplicate packets observed.
* Overlay consistent within 10 s.

### 6. Next Steps

* Add update messages (`UPDATE_META`, `ACK`, `NACK`) for Assignment 2.

### Assignment Summary

* Purpose: Build the UDP overlay for a decentralized chatbot.
* Core Deliverable: `node.py` implementing discovery, heartbeat, and logging using the required packet formats.
* Evaluation: 10 points based on correctness, stability, metrics, and report.
* Next Step: In Assignment 2, students will extend this overlay to exchange model updates (`UPDATE_META`, `UPDATE_CHUNK`, `ACK`, `NACK`) and begin distributed learning.


### Rubric
| #  | Criterion                           | Logical Behavior                                                                                               | Points |
| -- | ----------------------------------- | -------------------------------------------------------------------------------------------------------------- | ------ |
| 1  | Socket Initialization Works     | Node successfully binds to a UDP port and can both send and receive datagrams.                              | 1      |
| 2  | Broadcast Discovery Functional  | Node periodically sends `[PEER_SYNC]` messages and receives broadcast announcements from others on the subnet. | 1      |
| 3  | Peer Table Populates Correctly  | Node inserts new peers on first discovery and updates existing entries on repeated syncs.                      | 1      |
| 4  | Self-Filtering Implemented      | Node ignores its own broadcast packets (no self-loop entries).                                                 | 1      |
| 5  | Unicast Heartbeat Works         | Node sends `[PING]` to known peers and receives `[PONG]` replies.                                              | 1      |
| 6  | Timeout and Removal Logic Works | Peers that fail to reply within 30 s are removed from the active table.                                        | 1      |
| 7  | Packet Parsing Valid            | Incoming messages are correctly parsed into header + body; invalid or malformed packets are ignored safely.    | 1      |
| 8  | Logging and Status Output       | Node prints clear events for join, ping, timeout, and table summary (e.g., `[TABLE] 3 active`).                | 1      |
| 9  | Multi-Node Convergence          | When three or more nodes run, all peer tables converge to the same membership list within 10 seconds.          | 1      |
| 10 | Clean Shutdown and Restart      | Node stops gracefully on Ctrl-C without leaving open sockets; restarting rejoins the network normally.         | 1      |

