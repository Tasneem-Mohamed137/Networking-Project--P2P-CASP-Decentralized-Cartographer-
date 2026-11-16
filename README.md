
==============================================================
 CSE361 - Computer Networks - Fall 2026
 Project 1: P2P-CASP (Decentralized Cartographer)
 Phase 1 - Prototype
==============================================================

TEAM MEMBERS
------------
1. Tasneem Mohamed Shaban Nasr      - 24P0443
2. Ahmed Tamer Ahmed Abdelhalim     - 24P0189
3. Mazen Hesham Ibrahim Shahbunder  - 24P0432
4. Seif Emad Eldin Sallam           - 24P0386
5. Sama Tarek Mostafa               - 23P0451

DEMO VIDEO (5 minutes)
--------------------------------------------------------
VIDEO LINK: <PASTE YOUR VIDEO LINK HERE>


1. OVERVIEW
-----------
Phase 1 prototype of P2P-CASP over UDP. It demonstrates the INIT
(JOIN / ACK + peer list) and DATA (CHUNK_REQ / CHUNK_DATA) exchanges
between one tracker/seeder server and up to 4 clients, with
per-packet logging and an automated baseline test.

Implemented in Phase 1:
  - Custom binary header, 12 bytes (limit: <= 16 bytes)
  - Message types: JOIN, ACK (carries peer list), CHUNK_REQ, CHUNK_DATA
  - Server: bootstrap tracker + seeder with a hard 50 kbps outbound
    throttle (token bucket)
  - Client: join with retry, request all 256 chunks (64x64 grid),
    500 ms TTL with retransmission, SHA-256 verification of every
    chunk, duplicate suppression, final map verification
  - Packet logger (CSV, one row per sent/received/dropped packet)
  - Automated baseline script (server + 4 clients)

Planned for Phase 2 (not in this prototype): PEER_HEARTBEAT,
CHUNK_BITMAP, peer-to-peer chunk exchange, dynamic peer selection,
MAP_COMPLETE, netem scenarios.


2. REQUIREMENTS
---------------
  - Python 3.8+ (tested on Python 3.12, Windows)
  - Standard library only (socket, struct, hashlib, json, csv, ...)
  - No external networking libraries
  - Cross-platform: Windows and Linux.
    Windows: use "py"    Linux/macOS: use "python3"
  - Server listens on 127.0.0.1:9000 (UDP)


3. FILES
--------
  protocol.py   Header pack/unpack, packet build/parse, synthetic map
                generator, chunk manifest (SHA-256), peer-list encoding
  server.py     Tracker + throttled seeder
  client.py     Peer client (join, fetch 256 chunks, verify)
  logger.py     CSV packet logger
  baseline.py   Automated "Baseline (LAN)" scenario (server + 4 clients)
  README.txt    This file


4. HOW TO RUN
-------------
Put all files in the same folder and open a terminal there.

4.1 Protocol self-test
    py protocol.py
  Expected last line: All protocol self-tests passed.

4.2 Automated baseline test (server + 4 clients)
    py baseline.py                      (no throttle, fast sanity run)
    py baseline.py --throttle-kbps 50   (with the 50 kbps server limit)
    py baseline.py --runs 5             (5 repetitions, prints
                                         min / median / max)
  Expected: BASELINE RESULT: PASS
  Logs and summaries are written under logs/.

4.3 Manual run (one server, one client)
  Terminal 1:
    py server.py --log-dir logs\manual
  Terminal 2:
    py client.py 1 --log-dir logs\manual
  Expected client output: DONE: 256/256 chunks ... map verified: True
  Stop the server with Ctrl+C (it prints its statistics).
  Linux: use python3 and "logs/manual" instead of "logs\manual".

4.4 Error-handling checks
  a) Client without a server:
       py client.py 1 --log-dir logs\noserver
     Expected: "JOIN failed" and INCOMPLETE: 0/256 chunks after ~2.5 s,
     exit code 1.
  b) Malformed packet sent to a running server:
       py -c "import socket; s=socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.sendto(b'abc', ('127.0.0.1', 9000))"
     Expected: server keeps running; logs/.../server_packets.csv
     contains a "dropped malformed packet" row.


5. PROTOCOL SUMMARY (header: 12 bytes, network byte order)
-----------------------------------------------------------
  struct format: '!BBHBBHI'

  Offset  Size  Field            Description
  0       1     Protocol_ID      0xCA (magic)
  1       1     Version|Type     high 4 bits = version, low 4 = msg type
  2       2     Seq_Num          sequence number
  4       1     Peer_ID          sender peer id (0 = server)
  5       1     Flags            reserved / flags
  6       2     Payload_Length   bytes after the header
  8       4     Timestamp        sender timestamp (used for RTT)

  Message types: 0=JOIN, 1=ACK, 2=CHUNK_REQ, 3=CHUNK_DATA
  Peer-list entry (inside ACK payload): 7 bytes = peer_id(1) + IPv4(4)
  + port(2), struct format '!B4sH'.
  Max datagram size: <= 1200 bytes.


6. FLOW (Phase 1)
-----------------
  Client                          Server
    |------- JOIN --------------->|   register peer
    |<------ ACK + peer list -----|
    |------- CHUNK_REQ #i ------->|   (i = 0..255, one at a time)
    |<------ CHUNK_DATA #i -------|   hash verified by client
  Lost request/reply -> client retransmits after 500 ms TTL.


7. CONSTRAINTS COVERED IN THIS PROTOTYPE
----------------------------------------
  UDP only ....................... yes
  Header <= 16 bytes ............. yes (12)
  Datagram <= 1200 bytes ......... yes
  64x64 map = 256 chunks ......... yes
  Chunk TTL = 500 ms ............. yes
  Server throttle 50 kbps ........ yes (token bucket)
  Hash verification .............. yes (SHA-256 per chunk)
  Duplicate suppression .......... yes
  Log of all packets ............. yes (CSV)
  Standard libraries only ........ yes


8. KNOWN LIMITATIONS (Phase 1)
------------------------------
  - Clients fetch only from the server (stop-and-wait). With the 50 kbps
    throttle, 4 clients take about 12-14 s; the < 5 s baseline target
    is reached in Phase 2 once peers exchange chunks with each other.
  - pcap files and netem scenarios (loss, delay, churn) are part of
    Phase 2.
  - Measured RTT with the throttle on is inflated by the server rate
    limiter queueing (about 50 ms); without throttle it is ~0.2 ms.
