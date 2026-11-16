import csv
import time
from datetime import datetime
from pathlib import Path

import protocol

FIELDS = ["time_iso", "epoch_ms", "event", "direction", "msg_type",
          "seq", "peer_id", "size", "remote", "note"]


class PacketLogger:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.path, "w", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._file, fieldnames=FIELDS)
        self._writer.writeheader()
        self._file.flush()

    def _row(self, **values):
        now = time.time()
        row = {f: "" for f in FIELDS}
        row["time_iso"] = datetime.fromtimestamp(now).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        row["epoch_ms"] = int(now * 1000)
        row.update(values)
        self._writer.writerow(row)
        self._file.flush()   # flush every row so a killed process keeps its log

    def log_packet(self, direction, header, size, remote, note=""):
        self._row(event="PACKET", direction=direction,
                  msg_type=protocol.msg_name(header["msg_type"]),
                  seq=header["seq_num"], peer_id=header["peer_id"],
                  size=size, remote=f"{remote[0]}:{remote[1]}", note=note)

    def log_event(self, note, remote=None):
        self._row(event="EVENT", remote=f"{remote[0]}:{remote[1]}" if remote else "", note=note)

    def close(self):
        if not self._file.closed:
            self._file.close()