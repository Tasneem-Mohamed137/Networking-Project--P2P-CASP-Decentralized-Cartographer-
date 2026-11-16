import argparse
import hashlib
import json
import socket
import sys
import time
from pathlib import Path

import protocol
from logger import PacketLogger


class Client:
    def __init__(self, args):
        self.peer_id = args.peer_id
        self.server = (args.server_host, args.server_port)
        self.ttl = args.ttl_ms / 1000.0
        self.max_retries = args.max_retries
        self.num_chunks = args.chunks

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        log_dir = Path(args.log_dir)
        self.logger = PacketLogger(log_dir / f"client_{self.peer_id}_packets.csv")
        self.summary_path = log_dir / f"client_{self.peer_id}_summary.json"

        self.seq = 0
        self.received = {}          # chunk_id -> bytes
        self.peers = {}
        self.stats = {"requests_sent": 0, "retransmits": 0, "timeouts": 0,
                      "duplicates": 0, "hash_failures": 0, "malformed": 0,
                      "bytes_sent": 0, "bytes_received": 0}
        self.rtts = []

    def log(self, text):
        print(f"[client {self.peer_id}] {text}")

    def send(self, packet, note=""):
        self.sock.sendto(packet, self.server)
        self.stats["bytes_sent"] += len(packet)
        self.logger.log_packet("SEND", protocol.unpack_header(packet), len(packet), self.server, note)

    def recv_until(self, deadline):
        # next valid packet from the server before the deadline, else None
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            self.sock.settimeout(remaining)
            try:
                data, addr = self.sock.recvfrom(protocol.RECV_BUFSIZE)
            except socket.timeout:
                return None
            except ConnectionResetError:   # Windows ICMP noise
                continue
            if addr != self.server:
                self.logger.log_event("ignored packet from unknown sender", addr)
                continue
            try:
                header, payload = protocol.parse_packet(data)
            except ValueError as e:
                self.stats["malformed"] += 1
                self.logger.log_event(f"dropped malformed packet: {e}", addr)
                continue
            self.stats["bytes_received"] += len(data)
            self.logger.log_packet("RECV", header, len(data), addr)
            return header, payload

    def join(self):
        self.seq = protocol.next_seq(self.seq)
        join_seq = self.seq
        for attempt in range(1, self.max_retries + 1):
            pkt = protocol.build_packet(protocol.MSG_JOIN, join_seq, self.peer_id)
            self.send(pkt, note="" if attempt == 1 else f"retransmit #{attempt - 1}")
            deadline = time.monotonic() + self.ttl
            while True:
                result = self.recv_until(deadline)
                if result is None:
                    self.stats["timeouts"] += 1
                    break
                header, payload = result
                if header["msg_type"] == protocol.MSG_ACK and header["seq_num"] == join_seq:
                    try:
                        self.peers = protocol.decode_peer_list(payload)
                    except ValueError as e:
                        self.logger.log_event(f"bad peer list: {e}")
                        self.peers = {}
                    self.log(f"joined; peers known: {sorted(self.peers)}")
                    return True
        return False

    def accept_chunk(self, payload):
        # verify + store one CHUNK_DATA payload; returns chunk_id or None
        try:
            chunk_id, data = protocol.unpack_chunk_data(payload)
        except ValueError as e:
            self.stats["malformed"] += 1
            self.logger.log_event(f"bad CHUNK_DATA: {e}")
            return None
        if not 0 <= chunk_id < self.num_chunks:
            self.logger.log_event(f"CHUNK_DATA for unexpected chunk {chunk_id}")
            return None
        if chunk_id in self.received:   # duplicate suppression
            self.stats["duplicates"] += 1
            self.logger.log_event(f"duplicate chunk {chunk_id} discarded")
            return None
        if protocol.chunk_hash(data) != protocol.manifest()[chunk_id]:
            self.stats["hash_failures"] += 1
            self.logger.log_event(f"hash mismatch for chunk {chunk_id}, discarded")
            return None
        self.received[chunk_id] = data
        return chunk_id

    def fetch_chunk(self, chunk_id):
        for attempt in range(1, self.max_retries + 1):
            self.seq = protocol.next_seq(self.seq)
            pkt = protocol.build_packet(protocol.MSG_CHUNK_REQ, self.seq, self.peer_id,
                                        protocol.pack_chunk_req(chunk_id))
            self.stats["requests_sent"] += 1
            if attempt > 1:
                self.stats["retransmits"] += 1
            self.send(pkt, note=f"chunk {chunk_id}" + ("" if attempt == 1 else f" retransmit #{attempt - 1}"))

            deadline = time.monotonic() + self.ttl   # TTL (500 ms default)
            while chunk_id not in self.received:
                result = self.recv_until(deadline)
                if result is None:
                    self.stats["timeouts"] += 1
                    break
                header, payload = result
                if header["msg_type"] != protocol.MSG_CHUNK_DATA:
                    continue
                # a different chunk arriving here is simply out-of-order and still stored
                if self.accept_chunk(payload) is not None:
                    self.rtts.append((protocol.now_ms() - header["timestamp"]) & 0xFFFFFFFF)
            if chunk_id in self.received:
                return True
        return False

    def run(self):
        start = time.perf_counter()
        ok = False
        join_ms = None
        try:
            if not self.join():
                self.log("JOIN failed (no ACK from server)")
            else:
                join_ms = (time.perf_counter() - start) * 1000
                ok = True
                for chunk_id in range(self.num_chunks):
                    if chunk_id in self.received:
                        continue
                    if not self.fetch_chunk(chunk_id):
                        self.log(f"gave up on chunk {chunk_id}")
                        ok = False
                        break
        finally:
            elapsed_ms = (time.perf_counter() - start) * 1000
            map_ok = False
            if len(self.received) == protocol.NUM_CHUNKS:
                assembled = b"".join(self.received[i] for i in range(protocol.NUM_CHUNKS))
                map_ok = hashlib.sha256(assembled).digest() == protocol.full_map_hash()
            complete = ok and len(self.received) == self.num_chunks
            summary = {
                "peer_id": self.peer_id,
                "complete": complete,
                "map_verified": map_ok,
                "chunks_received": len(self.received),
                "completion_time_ms": round(elapsed_ms, 2) if complete else None,
                "join_time_ms": round(join_ms, 2) if join_ms is not None else None,
                "mean_rtt_ms": round(sum(self.rtts) / len(self.rtts), 3) if self.rtts else None,
                **self.stats,
            }
            self.summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
            self.logger.log_event(f"finished: {summary}")
            self.logger.close()
            self.sock.close()
            self.log(f"{'DONE' if complete else 'INCOMPLETE'}: {len(self.received)}/{self.num_chunks} "
                     f"chunks in {elapsed_ms:.0f} ms, map verified: {map_ok}, "
                     f"retransmits: {self.stats['retransmits']}, duplicates: {self.stats['duplicates']}")
        return 0 if complete else 1


def main():
    parser = argparse.ArgumentParser(description="P2P-CASP client (Phase 1)")
    parser.add_argument("peer_id", type=int, help="this client's id (1..254)")
    parser.add_argument("--server-host", default="127.0.0.1")
    parser.add_argument("--server-port", type=int, default=9000)
    parser.add_argument("--chunks", type=int, default=protocol.NUM_CHUNKS,
                        help="how many chunks to fetch (default: all 256)")
    parser.add_argument("--ttl-ms", type=float, default=500, help="chunk TTL before re-requesting")
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--log-dir", default="logs")
    args = parser.parse_args()

    if not 1 <= args.peer_id <= protocol.MAX_CLIENT_ID:
        parser.error(f"peer_id must be between 1 and {protocol.MAX_CLIENT_ID}")
    if not 1 <= args.chunks <= protocol.NUM_CHUNKS:
        parser.error(f"--chunks must be between 1 and {protocol.NUM_CHUNKS}")

    sys.exit(Client(args).run())


if __name__ == "__main__":
    main()