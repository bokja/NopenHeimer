import socket
import struct
import json
import re

# Config
DEFAULT_PORT = 25565
TIMEOUT = 0.4

# Regex to strip Minecraft color/formatting codes (e.g. §a, §l)
STRIP_FORMATTING = re.compile(r'§.')

def pack_varint(value):
    out = bytearray()
    while True:
        temp = value & 0b01111111
        value >>= 7
        if value:
            temp |= 0b10000000
        out.append(temp)
        if not value:
            break
    return out

def read_varint(sock):
    num = 0
    for i in range(5):
        byte = sock.recv(1)
        if not byte:
            return None
        byte_val = byte[0]
        num |= (byte_val & 0x7F) << (7 * i)
        if not (byte_val & 0x80):
            break
    return num

def sanitize_motd(motd):
    if not motd:
        return None
    motd = STRIP_FORMATTING.sub('', motd).strip()
    return motd if motd else None

def ping_modern(ip, port=DEFAULT_PORT, timeout=TIMEOUT):
    try:
        with socket.create_connection((ip, port), timeout=timeout) as s:
            # Handshake packet
            host_bytes = ip.encode('utf-8')
            packet = bytearray()
            packet += pack_varint(0x00)                      # Packet ID
            packet += pack_varint(754)                       # Protocol version (1.20.4)
            packet += pack_varint(len(host_bytes)) + host_bytes
            packet += struct.pack('>H', port)
            packet += pack_varint(1)                         # Next state: status

            s.sendall(pack_varint(len(packet)) + packet)     # Handshake
            s.sendall(b'\x01\x00')                            # Status request

            _ = read_varint(s)                               # Packet length
            _ = read_varint(s)                               # Packet ID
            json_len = read_varint(s)
            if json_len is None:
                return None

            data = b''
            while len(data) < json_len:
                chunk = s.recv(json_len - len(data))
                if not chunk:
                    break
                data += chunk

            result = json.loads(data.decode("utf-8"))

            # Parse result
            description = result.get("description", {})
            if isinstance(description, dict):
                motd = description.get("text", "")
            else:
                motd = str(description)
            motd = sanitize_motd(motd)

            players = result.get("players", {})
            version = result.get("version", {}).get("name", "unknown")

            sample = players.get("sample", [])
            player_names = [p.get("name") for p in sample if isinstance(p.get("name"), str)]
            if len(player_names) > 20:
                player_names = player_names[:20]

            return {
                "motd": motd,
                "players_online": players.get("online", 0),
                "players_max": players.get("max", 0),
                "version": version or "unknown",
                "player_names": player_names
            }

    except Exception as e:
        print(f"[!] Modern ping error for {ip}: {e}")
        return None

# ------------------------------------
# Legacy Ping (pre-1.7 Minecraft)
# ------------------------------------

def ping_legacy(ip, port=DEFAULT_PORT, timeout=TIMEOUT):
    try:
        with socket.create_connection((ip, port), timeout=timeout) as s:
            s.sendall(b'\xfe')
            return s.recv(1024)
    except Exception:
        return None

def parse_legacy_ping(response):
    try:
        data = response.decode("utf-16be")[3:].split("§")
        if len(data) >= 5:
            motd = sanitize_motd(data[0])
            return {
                "motd": motd,
                "players_online": int(data[1]),
                "players_max": int(data[2]),
                "version": data[3],
                "player_names": []  # Legacy doesn't return sample list
            }
    except Exception as e:
        print(f"[!] Legacy parse failed: {e}")
    return None

# ------------------------------------
# Unified Ping Entry Point
# ------------------------------------

def ping_server(ip, port=DEFAULT_PORT):
    """Attempt to ping server using modern protocol, fallback to legacy."""
    result = ping_modern(ip, port)
    if result:
        return result

    legacy = ping_legacy(ip, port)
    if legacy:
        return parse_legacy_ping(legacy)

    return None
