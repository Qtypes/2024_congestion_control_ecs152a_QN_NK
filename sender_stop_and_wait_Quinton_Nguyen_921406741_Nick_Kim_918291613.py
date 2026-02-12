import socket
import time
from decimal import Decimal, ROUND_CEILING
from pathlib import Path

PACKET_SIZE = 1024
SEQ_ID_SIZE = 4
MESSAGE_SIZE = PACKET_SIZE - SEQ_ID_SIZE
FINACK_BODY = b"==FINACK=="


def make_packet(seq_id, payload):
    return int.to_bytes(seq_id, SEQ_ID_SIZE, signed=True, byteorder="big") + payload


def parse_packet(packet):
    seq_id = int.from_bytes(packet[:SEQ_ID_SIZE], signed=True, byteorder="big")
    body = packet[SEQ_ID_SIZE:]
    return seq_id, body


def format_float(value):
    return format(
        Decimal(str(value)).quantize(Decimal("0.0000001"), rounding=ROUND_CEILING),
        "f",
    )

def run_stop_and_wait(file_bytes, host, port, timeout_s):
    packet_delays = []

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        transfer_start = time.perf_counter()
        sock.settimeout(timeout_s)

        seq_id = 0
        while seq_id < len(file_bytes):
            chunk = file_bytes[seq_id : seq_id + MESSAGE_SIZE]
            expected_ack = seq_id + len(chunk)
            packet = make_packet(seq_id, chunk)

            first_send_time = None
            while True:
                if first_send_time is None:
                    first_send_time = time.perf_counter()

                sock.sendto(packet, (host, port))

                try:
                    while True:
                        ack_packet, _ = sock.recvfrom(PACKET_SIZE)
                        ack_seq, ack_msg = parse_packet(ack_packet)
                        if ack_msg == b"ack" and ack_seq >= expected_ack:
                            packet_delays.append(time.perf_counter() - first_send_time)
                            seq_id = expected_ack
                            break
                    break
                except socket.timeout:
                    continue

        fin_seq_id = len(file_bytes)
        fin_packet = make_packet(fin_seq_id, b"")
        got_fin_ack = False
        got_fin_signal = False
        transfer_end = None

        while not (got_fin_ack and got_fin_signal):
            sock.sendto(fin_packet, (host, port))

            try:
                while True:
                    ctrl_packet, _ = sock.recvfrom(PACKET_SIZE)
                    ctrl_seq, ctrl_msg = parse_packet(ctrl_packet)

                    if ctrl_msg == b"ack" and ctrl_seq == fin_seq_id:
                        got_fin_ack = True
                        if transfer_end is None:
                            transfer_end = time.perf_counter()
                    elif ctrl_msg == b"fin":
                        got_fin_signal = True

                    if got_fin_ack and got_fin_signal:
                        break
            except socket.timeout:
                continue

        sock.sendto(make_packet(fin_seq_id, FINACK_BODY), (host, port))

    transfer_time = transfer_end - transfer_start
    throughput = len(file_bytes) / transfer_time
    avg_delay = sum(packet_delays) / len(packet_delays)
    metric = 0.3 * (throughput / 1000.0) + 0.7 * (1.0 / avg_delay)

    return throughput, avg_delay, metric


def main():
    host = "127.0.0.1"
    port = 5001
    timeout_s = 0.5
    file_path = Path(__file__).resolve().parent / "docker" / "file.mp3"

    file_bytes = file_path.read_bytes()
    throughput, avg_delay, metric = run_stop_and_wait(
        file_bytes=file_bytes,
        host=host,
        port=port,
        timeout_s=timeout_s,
    )

    print(
        f"{format_float(throughput)},{format_float(avg_delay)},{format_float(metric)}"
    )


if __name__ == "__main__":
    main()
