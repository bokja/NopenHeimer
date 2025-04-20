# NopenHeimer/shared/mc_ping.py - IMPROVED Version
import socket
import struct
import json
import re
import time

# Use shared logger and config
from shared.logger import get_logger
from shared.config import TARGET_PORT as DEFAULT_PORT # Use port from config
# Note: Timeout is passed explicitly by worker now, no need for global default here

log = get_logger("mc_ping")

# Regex to strip Minecraft color/formatting codes
STRIP_FORMATTING = re.compile(r'§[0-9a-fk-or]') # More specific regex

# --- Varint Helpers ---
def pack_varint(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value != 0:
            byte |= 0x80
        out.append(byte)
        if value == 0:
            break
    return bytes(out)

def read_varint(sock):
    number = 0
    shift = 0
    bytes_read = 0
    while True:
        try:
            byte = sock.recv(1)
            if not byte:
                log.debug("read_varint: Socket closed unexpectedly.")
                return None # Indicate connection closed
            val = byte[0]
            number |= (val & 0x7F) << shift
            shift += 7
            bytes_read += 1
            if not (val & 0x80):
                break
            if bytes_read > 5: # VarInts should not be longer than 5 bytes
                log.warning("read_varint: VarInt exceeded 5 bytes, potential data corruption.")
                return None
        except socket.timeout:
            log.debug("read_varint: Socket timeout.")
            return None # Indicate timeout
        except OSError as e:
            log.debug(f"read_varint: Socket error: {e}")
            return None # Indicate socket error
    return number

def sanitize_motd(motd):
    if not motd:
        return "" # Return empty string instead of None
    # Handle complex MOTD structures (common in modern MC)
    text = ""
    if isinstance(motd, dict):
        if 'extra' in motd:
            text = "".join(part.get("text", "") for part in motd['extra'])
        if not text and 'text' in motd:
             text = motd.get("text", "")
    elif isinstance(motd, list): # Sometimes seen?
         text = "".join(part.get("text", "") for part in motd if isinstance(part, dict))
    elif isinstance(motd, str):
        text = motd
    else:
         text = str(motd) # Fallback

    return STRIP_FORMATTING.sub('', text).strip()

# --- Modern Ping ---
def ping_modern(ip, port=DEFAULT_PORT, timeout=0.4):
    log.debug(f"Attempting modern ping: {ip}:{port} (Timeout: {timeout}s)")
    start_time = time.monotonic()
    conn = None
    try:
        # Explicit connect/close for clarity and resource management
        conn = socket.create_connection((ip, port), timeout=timeout)

        # Handshake Packet (Constants can be updated, e.g., protocol version)
        protocol_version = 765 # Example: 1.20.4 (Check https://wiki.vg/Protocol_version_numbers)
        packet = bytearray()
        packet += pack_varint(0x00) # Packet ID: Handshake
        packet += pack_varint(protocol_version)
        ip_bytes = ip.encode('utf-8')
        packet += pack_varint(len(ip_bytes))
        packet += ip_bytes
        packet += struct.pack('>H', port) # Port (unsigned short)
        packet += pack_varint(1) # Next state: Status

        # Send Handshake
        conn.sendall(pack_varint(len(packet)) + packet)

        # Send Status Request Packet (Packet ID: 0x00)
        conn.sendall(pack_varint(1) + b'\x00')

        # --- Read Response ---
        response_packet_length = read_varint(conn)
        if response_packet_length is None or response_packet_length <= 0:
            log.debug(f"Modern ping {ip}:{port}: Invalid/Timeout reading response length.")
            return None

        response_packet_id = read_varint(conn)
        if response_packet_id is None:
             log.debug(f"Modern ping {ip}:{port}: Invalid/Timeout reading response ID.")
             return None

        if response_packet_id != 0x00: # Packet ID for Status Response should be 0x00
            log.warning(f"Modern ping {ip}:{port}: Unexpected packet ID received: {response_packet_id:#x}")
            return None

        json_length = read_varint(conn)
        if json_length is None or json_length <= 0:
             log.debug(f"Modern ping {ip}:{port}: Invalid/Timeout reading JSON length.")
             return None

        json_data = b''
        bytes_to_read = json_length
        while bytes_to_read > 0:
            chunk = conn.recv(min(bytes_to_read, 4096)) # Read in chunks
            if not chunk:
                log.warning(f"Modern ping {ip}:{port}: Socket closed while reading JSON data.")
                return None
            json_data += chunk
            bytes_to_read -= len(chunk)

        # --- Parse JSON ---
        try:
            result = json.loads(json_data.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            log.warning(f"Modern ping {ip}:{port}: Failed to decode JSON response: {e}")
            return None

        # --- Extract Data ---
        motd = sanitize_motd(result.get("description", ""))
        players = result.get("players", {})
        version = result.get("version", {}).get("name", "") # Use empty string as default
        sample = players.get("sample", [])
        player_names = [p.get("name", "") for p in sample if isinstance(p, dict) and p.get("name")]
        if len(player_names) > 20: player_names = player_names[:20]

        log.debug(f"Modern ping {ip}:{port} successful in {time.monotonic() - start_time:.3f}s")
        return {
            "motd": motd,
            "players_online": players.get("online", 0),
            "players_max": players.get("max", 0),
            "version": version or "unknown", # Ensure not empty
            "player_names": player_names
        }

    except socket.timeout:
        log.debug(f"Modern ping {ip}:{port}: Connection timed out ({timeout}s).")
        return None
    except ConnectionRefusedError:
        log.debug(f"Modern ping {ip}:{port}: Connection refused.")
        return None
    except OSError as e:
        log.debug(f"Modern ping {ip}:{port}: OS Error: {e}")
        return None
    except Exception as e:
        log.error(f"Modern ping {ip}:{port}: Unexpected error: {e}", exc_info=False) # Log error but not full traceback usually
        return None
    finally:
         if conn:
             try: conn.close()
             except Exception: pass

# --- Legacy Ping ---
def ping_legacy(ip, port=DEFAULT_PORT, timeout=0.4):
    log.debug(f"Attempting legacy ping: {ip}:{port} (Timeout: {timeout}s)")
    conn = None
    try:
        conn = socket.create_connection((ip, port), timeout=timeout)
        # Basic FE ping (some servers need FE 01)
        conn.sendall(b'\xfe\x01') # Try FE 01 first, most common
        # Or just conn.sendall(b'\xfe')
        data = conn.recv(1024)
        log.debug(f"Legacy ping {ip}:{port} received {len(data)} bytes.")
        return data
    except socket.timeout:
         log.debug(f"Legacy ping {ip}:{port}: Connection timed out ({timeout}s).")
         return None
    except OSError as e: # Catch specific socket errors
        log.debug(f"Legacy ping {ip}:{port}: OS Error: {e}")
        return None
    except Exception as e:
        log.error(f"Legacy ping {ip}:{port}: Unexpected error: {e}", exc_info=False)
        return None
    finally:
         if conn:
             try: conn.close()
             except Exception: pass


def parse_legacy_ping(response):
    """Parses different potential legacy response formats."""
    if not response: return None

    try:
        # Kick Packet (Starts with FF)
        if response.startswith(b'\xff'):
             parts = response[1:].decode('utf-16be', errors='ignore').split('\xa7') # § is C2 A7 in UTF-8, but \xa7 used often? check encoding
             if len(parts) >= 1: # Just get kick message
                 log.debug(f"Parsed legacy kick packet: {parts[0]}")
                 # Decide if this counts as a 'found' server - probably not useful info
                 # return {"motd": f"[KICK] {parts[0]}", ...} # Return minimal info if needed
                 return None # Treat kick as not found for now
        # FE Response (Starts with FE after potentially FF 01)
        elif b'\x00' in response: # Look for null terminators common in FE 01 response
            # Find the start after identifier/length bytes
            start_index = -1
            if response.startswith(b'\xff\x00'): # Could be FF length ...
                start_index = response.find(b'\x00\x00\x00', 1) # Look for pattern after length
                if start_index != -1: start_index += 3
            elif response.startswith(b'\xfe\x01\xfa'): # Another pattern
                 start_index = 3
            # Add other known patterns if needed

            if start_index != -1:
                 parts = response[start_index:].decode('utf-16be', errors='ignore').split('\x00')
                 # Expected format: ProtocolVer, VersionStr, MOTD, Online, Max
                 if len(parts) >= 6:
                     log.debug(f"Parsed legacy FE01 response: V:{parts[1]} MOTD:{parts[3][:30]}...")
                     return {
                         "motd": sanitize_motd(parts[3]),
                         "players_online": int(parts[4]),
                         "players_max": int(parts[5]),
                         "version": parts[1] + " / " + parts[2], # Combine versions
                         "player_names": []
                     }
        # Simplest § separated format (Less common now?)
        elif b'\xa7' in response: # Check for § separator bytes
             # Find first § after potential header bytes
             sep_index = response.find(b'\xa7')
             if sep_index != -1:
                 # Decoding might depend on server encoding
                 data = response[sep_index+1:].decode('latin-1', errors='ignore').split('§') # Try latin-1? or utf-8?
                 if len(data) >= 3:
                     log.debug(f"Parsed legacy § response: MOTD:{data[0][:30]}...")
                     return {
                         "motd": sanitize_motd(data[0]),
                         "players_online": int(data[1]),
                         "players_max": int(data[2]),
                         "version": "Legacy",
                         "player_names": []
                     }

    except (ValueError, IndexError, UnicodeDecodeError, TypeError) as e:
        log.warning(f"Legacy parse failed: {e} (Data: {response[:50]}...)") # Show partial data on error
    except Exception as e:
        log.error(f"Unexpected legacy parse error: {e}", exc_info=False)

    log.debug("Could not parse legacy response.")
    return None


# --- Unified Ping Entry Point ---
def ping_server(ip, port=DEFAULT_PORT, timeout=0.4): # Use passed timeout
    """Attempt modern ping, fallback to legacy if needed."""
    modern_result = ping_modern(ip, port, timeout=timeout)
    if modern_result is not None: # Explicitly check for None, as empty dict is valid
        return modern_result

    legacy_response = ping_legacy(ip, port, timeout=timeout)
    if legacy_response:
        parsed_legacy = parse_legacy_ping(legacy_response)
        if parsed_legacy:
            return parsed_legacy

    log.debug(f"No successful ping (modern or legacy) for {ip}:{port}")
    return None