import zlib
import time

def calculate_crc32(packet_str: str) -> str:
    """
    Calculate CRC32 over the packet string.
    Returns 8-char hexadecimal string.
    """
    crc = zlib.crc32(packet_str.encode()) & 0xFFFFFFFF
    return f"{crc:08x}"

def parse_packet(packet_str: str) -> dict:
    """
    Parse a packet of the format: [HEADER]|[BODY]|[CRC32]
    Returns dict with keys: header, body, crc, valid
    """
    try:
        parts = packet_str.strip().split("|")
        if len(parts) < 7:
            return {"valid": False}
        *header_parts, body, crc = parts
        header = {
            "MSGTYPE": header_parts[0],
            "SENDER_ID": header_parts[1],
            "SEQ_ID": int(header_parts[2]),
            "TIMESTAMP": int(header_parts[3]),
            "MODEL_VER": int(header_parts[4]),
            "TTL": int(header_parts[5])
        }
        calc_crc = calculate_crc32("|".join(header_parts + [body]))
        return {
            "valid": (calc_crc.lower() == crc.lower()),
            "header": header,
            "body": body,
            "crc": crc
        }
    except Exception:
        return {"valid": False}

def current_millis() -> int:
    """Return current time in milliseconds"""
    return int(time.time() * 1000)
