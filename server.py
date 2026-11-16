import argparse
import socket
import time
from pathlib import Path

import protocol
from logger import PacketLogger


class Throttle:
    # token bucket: hard cap on outbound bytes/second (0 = off)
    def __init__(self, kbps):
        self.rate = kbps * 1000 / 8.0
        self.tokens = 0.0
        self.last = time.monotonic()

    def wait_for(self, nbytes):
        if self.rate <= 0:
            return
        while True:
            now = time.monotonic()
            self.tokens = min(float(nbytes), self.tokens + (now - self.last) * self.rate)
            self.last = now
            if self.tokens >= nbytes:
                self.tokens -= nbytes
                return
            time.sleep((nbytes - self.tokens) / self.rate)


class Server:
    def __init__(self, host, port, throttle_kbps, log_dir):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((host, port))
        self.sock.settimeout(1.0)   # so Ctrl+C works on Windows
        self.addr = (host, port)
        self.throttle = Throttle(throttle_kbps)
        self.logger = PacketLogger(Path(log_dir) / "server_packets.csv")
        self.peers = {}             # peer_id -> (ip, port)
        self.chunks = [protocol.generate_chunk(i) for i in range(protocol.NUM_CHUNKS)]
        self.stats = {"packets_in": 0, "packets_out": 0, "bytes_out": 0,
                      "malformed": 0, "joins": 0, "chunk_reqs": 0, "ignored": 0}

    def send(self, packet, addr, note=""):
        self.throttle.wait_for(len(packet))
        self.sock.sendto(packet, addr)
        self.stats["packets_out"] += 1
        self.stats["bytes_out"] += len(packet)
        self.logger.log_packet("SEND", protocol.unpack_header(packet), len(packet), addr, note)

    def handle_join(self, header, addr):
        pid = header["peer_id"]
        if not 1 <= pid <= protocol.MAX_CLIENT_ID:
            self.stats["ignored"] += 1
            self.logger.log_event(f"JOIN ignored: invalid peer_id {pid}", addr)
            return
        self.stats["joins"] += 1
        self.peers[pid] = addr      # repeated JOIN just refreshes the address
        print(f"[server] peer {pid} registered from {addr[0]}:{addr[1]} "
              f"({len(self.peers)} peers known)")

        listing = dict(list(self.peers.items())[:protocol.MAX_PEERS_PER_PACKET])
        payload = protocol.encode_peer_list(listing)
        # echo seq + timestamp so the client can match the reply and measure RTT
        ack = protocol.build_packet(protocol.MSG_ACK, header["seq_num"],
                                    protocol.SERVER_PEER_ID, payload,
                                    timestamp=header["timestamp"])
        self.send(ack, addr, note=f"peer list: {len(listing)} peers")

    def handle_chunk_req(self, header, payload, addr):
        pid = header["peer_id"]
        if self.peers.get(pid) != addr:
            self.stats["ignored"] += 1
            self.logger.log_event(f"CHUNK_REQ ignored: peer {pid} not registered from this address", addr)
            return
        try:
            chunk_id = protocol.unpack_chunk_req(payload)
        except ValueError as e:
            self.stats["malformed"] += 1
            self.logger.log_event(f"CHUNK_REQ dropped: {e}", addr)
            return
        if not 0 <= chunk_id < protocol.NUM_CHUNKS:
            self.stats["ignored"] += 1
            self.logger.log_event(f"CHUNK_REQ ignored: chunk {chunk_id} out of range", addr)
            return
        self.stats["chunk_reqs"] += 1
        data_pkt = protocol.build_packet(protocol.MSG_CHUNK_DATA, header["seq_num"],
                                         protocol.SERVER_PEER_ID,
                                         protocol.pack_chunk_data(chunk_id, self.chunks[chunk_id]),
                                         timestamp=header["timestamp"])
        self.send(data_pkt, addr, note=f"chunk {chunk_id}")

    def serve_forever(self):
        limit = "off" if self.throttle.rate <= 0 else f"{self.throttle.rate * 8 / 1000:.0f} kbps"
        print(f"[server] {protocol.PROTOCOL_NAME} v{protocol.VERSION} listening on "
              f"{self.addr[0]}:{self.addr[1]} (throttle: {limit})")
        try:
            while True:
                try:
                    data, addr = self.sock.recvfrom(protocol.RECV_BUFSIZE)
                except socket.timeout:
                    continue
                except ConnectionResetError:   # Windows: ICMP from a client that already exited
                    continue

                self.stats["packets_in"] += 1
                try:
                    header, payload = protocol.parse_packet(data)
                except ValueError as e:
                    self.stats["malformed"] += 1
                    self.logger.log_event(f"dropped malformed packet ({len(data)} bytes): {e}", addr)
                    continue

                self.logger.log_packet("RECV", header, len(data), addr)
                if header["msg_type"] == protocol.MSG_JOIN:
                    self.handle_join(header, addr)
                elif header["msg_type"] == protocol.MSG_CHUNK_REQ:
                    self.handle_chunk_req(header, payload, addr)
                else:
                    self.stats["ignored"] += 1
                    self.logger.log_event(
                        f"ignored message type {protocol.msg_name(header['msg_type'])}", addr)
        except KeyboardInterrupt:
            print("\n[server] stopping (Ctrl+C)")
        finally:
            print("[server] stats:", self.stats)
            self.logger.log_event(f"server stopped, stats={self.stats}")
            self.logger.close()
            self.sock.close()


def main():
    parser = argparse.ArgumentParser(description="P2P-CASP tracker / seeder (Phase 1)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--throttle-kbps", type=float, default=0,
                        help="outbound cap in kbps (0 = off, project limit is 50)")
    parser.add_argument("--log-dir", default="logs")
    args = parser.parse_args()
    Server(args.host, args.port, args.throttle_kbps, args.log_dir).serve_forever()


if __name__ == "__main__":
    main()