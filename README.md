P2P-CASP  -  Phase 1 prototype (Computer Networks Major Task, Fall 2026, Project 1)
=====================================================================================

Team members : <names + IDs>
Demo video   : <PASTE LINK HERE>      (access must be "anyone with the link can view")

What Phase 1 demonstrates
-------------------------
* INIT  : client sends JOIN over UDP, server registers the peer and answers ACK
          carrying the peer list (tracker role).
* DATA  : client sends CHUNK_REQ, server answers CHUNK_DATA. The client verifies
          every chunk against a SHA-256 manifest, re-requests after a 500 ms TTL,
          discards duplicates, and finally verifies the whole 64x64 map.
* Every sent/received packet is logged to CSV with timestamp and metadata.
*
*
* baseline.py runs the "Baseline (local)" scenario: 1 server + 4 clients.

Not in Phase 1 (planned for Phase 2): PEER_HEARTBEAT, CHUNK_BITMAP, MAP_COMPLETE,
peer-to-peer chunk exchange, dynamic peer selection, netem scenarios, pcap capture.
Their message-type IDs are already reserved in protocol.py.

Requirements
------------
* Python 3.8 or newer (developed on Windows with Python 3.14, also tested on Linux).
* Standard library only: socket, struct, hashlib, argparse, csv, json ...
  No external networking libraries.

Files
-----
protocol.py      header pack/unpack, message types, peer-list encoding, synthetic map
server.py        tracker + seeder (optional --throttle-kbps)
client.py        client (JOIN, fetch all chunks, verify, write summary)
logger.py        CSV packet logger
baseline.py  automated baseline scenario (server + 4 clients)
logs/            created automatically; one sub-folder per run

How to run
----------
1) Quick self-test of the wire format:
       py protocol.py

2) Baseline scenario (recommended):
       py baseline.py
   Options:  --runs 5   --clients 4   --port 9000   --throttle-kbps 50
   Output goes to logs/baseline_<timestamp>_run<N>/ (packet CSVs, summaries, console output).
   Prints PASS/FAIL at the end.

4) Manually, in separate terminals (on Linux/macOS use python3 instead of py):
       py server.py
       py client.py 1
       py client.py 2
   Stop the server with Ctrl+C.
   Server options: --host --port --throttle-kbps --log-dir
   Client options: --server-host --server-port --chunks --ttl-ms --max-retries --log-dir

Wire format (header = 12 bytes, network byte order, struct '!BBHBBHI')
----------------------------------------------------------------------
offset size field
  0     1   Protocol_ID     0xCA
  1     1   Version (high 4 bits) | Message_Type (low 4 bits)
  2     2   Sequence_Num    wraps at 65536
  4     1   Peer_ID         0 = server, 1..254 = clients
  5     1   Flags           reserved (0)
  6     2   Payload_Length  bytes after the header
  8     4   Timestamp       sender clock in ms (mod 2^32), echoed by the server for RTT

Message types: 0 JOIN, 1 ACK, 2 CHUNK_REQ, 3 CHUNK_DATA
               (reserved: 4 PEER_HEARTBEAT, 5 CHUNK_BITMAP, 6 MAP_COMPLETE)
Payloads:      ACK        = N x 7 bytes  [peer_id(1) | IPv4(4) | port(2)]
               CHUNK_REQ  = chunk_id (2 bytes)
               CHUNK_DATA = chunk_id (2 bytes) + 64 bytes of chunk data
Limits:        header <= 16 bytes; whole datagram (header + payload) <= 1200 bytes.

Synthetic map: 64x64 grid = 256 chunks (4x4 tiles, 4 bytes per cell = 64 bytes per chunk).
Chunks are generated deterministically (SHAKE-256 of a fixed seed + chunk index), so every
machine produces identical data and the client can verify content hashes.

Log format (CSV)
----------------
time_iso, epoch_ms, event (PACKET/EVENT), direction (SEND/RECV), msg_type, seq, peer_id,
size, remote, note.   Files: server_packets.csv, client_<id>_packets.csv.
Per-client results: client_<id>_summary.json (completion_time_ms, retransmits, duplicates, ...).

Reproducibility
---------------
Chunk generation is deterministic (fixed seed in protocol.py), no randomness is used.
For the netem scenarios (Phase 2) the commands and scripts will be added here:
    sudo tc qdisc add dev <IF> root netem loss 5%
    sudo tc qdisc del dev <IF> root

Known limitations of the Phase 1 prototype
------------------------------------------
* Clients fetch every chunk from the server only (stop-and-wait, one request in flight).
* RTT is measured with millisecond resolution, so on localhost it shows 0-1 ms.
* IPv4 only. The server throttle counts UDP payload bytes (header + payload), not IP/UDP headers.
* With --throttle-kbps 50 and 4 clients, the baseline takes about 12 s because all data comes
  from the server; the < 5 s acceptance target needs the Phase 2 peer-to-peer exchange.
* PROTOCOL_NAME in protocol.py is a placeholder - the team must choose its own unique name.
