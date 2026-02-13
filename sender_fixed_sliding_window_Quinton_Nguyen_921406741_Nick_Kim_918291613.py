"""
Fixed sliding window UDP sender. Window size = 100 packets.
Sends to receiver at localhost:5001. Packet size 1024 bytes (4-byte seq + 1020 payload).
"""
import socket
import time
from decimal import Decimal, ROUND_CEILING
from pathlib import Path

PACKET_SIZE = 1024
SEQ_ID_SIZE = 4
MESSAGE_SIZE = PACKET_SIZE - SEQ_ID_SIZE
WINDOW_PACKETS = 100
WINDOW_BYTES = WINDOW_PACKETS * MESSAGE_SIZE
FINACK_BODY = b"==FINACK=="
TIMEOUT_S = 0.5


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


def run_fixed_sliding_window(file_bytes, host, port):
    packet_delays = []  # delay for each chunk when acked
    first_send_times = {}  # seq_id -> first send time

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        transfer_start = time.perf_counter()
        sock.settimeout(TIMEOUT_S)

        send_base = 0  # first byte index of oldest unacked packet
        next_seq = 0   # next byte index to send
        file_len = len(file_bytes)
        transfer_end = None

        while send_base < file_len:
            # Send up to WINDOW_PACKETS in flight
            while next_seq < file_len and (next_seq - send_base) < WINDOW_BYTES:
                chunk = file_bytes[next_seq : next_seq + MESSAGE_SIZE]
                packet = make_packet(next_seq, chunk)
                if next_seq not in first_send_times:
                    first_send_times[next_seq] = time.perf_counter()
                sock.sendto(packet, (host, port))
                next_seq += len(chunk)

            try:
                ack_packet, _ = sock.recvfrom(PACKET_SIZE)
                ack_seq, ack_msg = parse_packet(ack_packet)

                if ack_msg == b"ack":
                    # Cumulative ACK: all bytes before ack_seq are received
                    new_send_base = max(send_base, ack_seq)
                    # Record delays for newly acked chunks
                    pos = send_base
                    while pos < new_send_base and pos < file_len:
                        if pos in first_send_times:
                            packet_delays.append(time.perf_counter() - first_send_times[pos])
                            del first_send_times[pos]
                        chunk_len = min(MESSAGE_SIZE, file_len - pos)
                        pos += chunk_len
                    send_base = new_send_base

            except socket.timeout:
                # Resend all unacked packets in window (go-back-N)
                pos = send_base
                while pos < next_seq and pos < file_len:
                    chunk = file_bytes[pos : pos + MESSAGE_SIZE]
                    sock.sendto(make_packet(pos, chunk), (host, port))
                    pos += len(chunk)

        # Send empty packet to signal end (fin_seq_id = file_len)
        fin_seq_id = file_len
        fin_packet = make_packet(fin_seq_id, b"")
        got_fin_ack = False
        got_fin_signal = False

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
    throughput = file_len / transfer_time
    avg_delay = sum(packet_delays) / len(packet_delays) if packet_delays else 0.0
    metric = 0.3 * (throughput / 1000.0) + 0.7 * (1.0 / avg_delay) if avg_delay else 0.0
    return throughput, avg_delay, metric


def main():
    host = "127.0.0.1"
    port = 5001
    file_path = Path(__file__).resolve().parent / "docker" / "file.mp3"
    file_bytes = file_path.read_bytes()

    throughput, avg_delay, metric = run_fixed_sliding_window(
        file_bytes=file_bytes, host=host, port=port
    )

    print(format_float(throughput))
    print(format_float(avg_delay))
    print(format_float(metric))


if __name__ == "__main__":
    main()
