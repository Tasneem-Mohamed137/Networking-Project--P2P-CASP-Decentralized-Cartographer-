import functools
import hashlib
import socket
import struct
import time

PROTOCOL_NAME = "P2P-CASP"   # TODO: your group's unique name
PROTOCOL_ID = 0xCA
VERSION = 1

HEADER_FORMAT = "!BBHBBHI"   # 12 bytes
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
assert HEADER_SIZE <= 16

MAX_DATAGRAM = 1200          # header + payload
MAX_PAYLOAD = MAX_DATAGRAM - HEADER_SIZE
RECV_BUFSIZE = 2048
SEQ_MOD = 1 << 16
SERVER_PEER_ID = 0
MAX_CLIENT_ID = 254

# message types (4 bits)
MSG_JOIN = 0
MSG_ACK = 1
MSG_CHUNK_REQ = 2
MSG_CHUNK_DATA = 3
MSG_PEER_HEARTBEAT = 4       # reserved for Phase 2
MSG_CHUNK_BITMAP = 5         # reserved for Phase 2
MSG_MAP_COMPLETE = 6         # reserved for Phase 2

MSG_NAMES = {
    MSG_JOIN: "JOIN",
    MSG_ACK: "ACK",
    MSG_CHUNK_REQ: "CHUNK_REQ",
    MSG_CHUNK_DATA: "CHUNK_DATA",
    MSG_PEER_HEARTBEAT: "PEER_HEARTBEAT",
    MSG_CHUNK_BITMAP: "CHUNK_BITMAP",
    MSG_MAP_COMPLETE: "MAP_COMPLETE",
}


def msg_name(msg_type):
    return MSG_NAMES.get(msg_type, f"UNKNOWN({msg_type})")


def now_ms():
    return int(time.time() * 1000) & 0xFFFFFFFF


def next_seq(seq):
    return (seq + 1) % SEQ_MOD


def _check_range(name, value, max_value):
    if not isinstance(value, int) or not 0 <= value <= max_value:
        raise ValueError(f"{name} out of range (0..{max_value}): {value!r}")


def pack_header(msg_type, seq_num, peer_id, flags, payload_length, timestamp):
    _check_range("msg_type", msg_type, 0x0F)
    _check_range("seq_num", seq_num, 0xFFFF)
    _check_range("peer_id", peer_id, 0xFF)
    _check_range("flags", flags, 0xFF)
    _check_range("payload_length", payload_length, 0xFFFF)
    _check_range("timestamp", timestamp, 0xFFFFFFFF)

    version_and_type = (VERSION << 4) | msg_type
    return struct.pack(HEADER_FORMAT, PROTOCOL_ID, version_and_type, seq_num,
                       peer_id, flags, payload_length, timestamp)


def unpack_header(data):
    if len(data) < HEADER_SIZE:
        raise ValueError("packet too short")

    values = struct.unpack(HEADER_FORMAT, data[:HEADER_SIZE])
    protocol_id = values[0]
    version_and_type = values[1]
    seq_num = values[2]
    peer_id = values[3]
    flags = values[4]
    payload_length = values[5]
    timestamp = values[6]

    if protocol_id != PROTOCOL_ID:
        raise ValueError("invalid protocol ID")

    version = version_and_type >> 4
    msg_type = version_and_type & 0x0F
    if version != VERSION:
        raise ValueError(f"unsupported version {version}")

    return {
        "protocol_id": protocol_id,
        "version": version,
        "msg_type": msg_type,
        "seq_num": seq_num,
        "peer_id": peer_id,
        "flags": flags,
        "payload_length": payload_length,
        "timestamp": timestamp,
    }


def build_packet(msg_type, seq_num, peer_id, payload=b"", flags=0, timestamp=None):
    if len(payload) > MAX_PAYLOAD:
        raise ValueError(f"payload too large ({len(payload)} > {MAX_PAYLOAD})")
    if timestamp is None:
        timestamp = now_ms()
    return pack_header(msg_type, seq_num, peer_id, flags, len(payload), timestamp) + payload


def parse_packet(data):
    if len(data) > MAX_DATAGRAM:
        raise ValueError("datagram larger than MTU limit")
    header = unpack_header(data)
    payload = data[HEADER_SIZE:]
    if len(payload) != header["payload_length"]:
        raise ValueError("payload length mismatch")
    return header, payload


# peer list entry: peer_id (1) + IPv4 (4) + port (2)
PEER_ENTRY_FORMAT = "!B4sH"
PEER_ENTRY_SIZE = struct.calcsize(PEER_ENTRY_FORMAT)
MAX_PEERS_PER_PACKET = MAX_PAYLOAD // PEER_ENTRY_SIZE


def encode_peer_list(peers):
    if len(peers) > MAX_PEERS_PER_PACKET:
        raise ValueError("too many peers for one packet")
    payload = b""
    for pid, (ip, port) in peers.items():
        payload += struct.pack(PEER_ENTRY_FORMAT, pid, socket.inet_aton(ip), port)
    return payload


def decode_peer_list(payload):
    if len(payload) % PEER_ENTRY_SIZE != 0:
        raise ValueError("peer list length is not a multiple of 7")
    peers = {}
    for i in range(0, len(payload), PEER_ENTRY_SIZE):
        pid, ip_raw, port = struct.unpack(PEER_ENTRY_FORMAT, payload[i:i + PEER_ENTRY_SIZE])
        peers[pid] = (socket.inet_ntoa(ip_raw), port)
    return peers


# synthetic map: 64x64 grid = 256 chunks of 64 bytes
GRID_SIZE = 64
NUM_CHUNKS = 256
CHUNK_SIZE = 64
MAP_SEED = 1337


def generate_chunk(chunk_id):
    if not 0 <= chunk_id < NUM_CHUNKS:
        raise ValueError(f"chunk_id out of range: {chunk_id}")
    seed = b"CASP-MAP:" + MAP_SEED.to_bytes(4, "big") + chunk_id.to_bytes(2, "big")
    return hashlib.shake_256(seed).digest(CHUNK_SIZE)


def chunk_hash(data):
    return hashlib.sha256(data).digest()


@functools.lru_cache(maxsize=1)
def manifest():
    return tuple(chunk_hash(generate_chunk(i)) for i in range(NUM_CHUNKS))


def full_map_hash():
    return hashlib.sha256(b"".join(generate_chunk(i) for i in range(NUM_CHUNKS))).digest()


def pack_chunk_req(chunk_id):
    return struct.pack("!H", chunk_id)


def unpack_chunk_req(payload):
    if len(payload) != 2:
        raise ValueError("CHUNK_REQ payload must be 2 bytes")
    return struct.unpack("!H", payload)[0]


def pack_chunk_data(chunk_id, data):
    return struct.pack("!H", chunk_id) + data


def unpack_chunk_data(payload):
    if len(payload) != 2 + CHUNK_SIZE:
        raise ValueError("CHUNK_DATA payload has wrong size")
    return struct.unpack("!H", payload[:2])[0], payload[2:]


if __name__ == "__main__":
    # quick self-test
    pkt = build_packet(MSG_CHUNK_REQ, 7, 3, pack_chunk_req(42), timestamp=12345)
    header, payload = parse_packet(pkt)
    assert header["msg_type"] == MSG_CHUNK_REQ and header["seq_num"] == 7
    assert header["peer_id"] == 3 and header["timestamp"] == 12345
    assert unpack_chunk_req(payload) == 42

    peers = {1: ("127.0.0.1", 5000), 2: ("10.0.0.5", 6000)}
    assert decode_peer_list(encode_peer_list(peers)) == peers
    assert len(manifest()) == NUM_CHUNKS

    for bad in (b"abc", b"\x00" * 12):
        try:
            unpack_header(bad)
        except ValueError as e:
            print("rejected bad packet:", e)

    print("Header size:", HEADER_SIZE, "| max payload:", MAX_PAYLOAD)
    print("Sample header hex:", pkt[:HEADER_SIZE].hex(" "))
    print("All protocol self-tests passed.")