# NopenHeimer/shared/mc_ping.py - IMPROVED Version
import socket, struct, json, re, time
from shared.logger import get_logger
from shared.config import TARGET_PORT as DEFAULT_PORT
log = get_logger("mc_ping")
STRIP_FORMATTING = re.compile(r'§[0-9a-fk-or]')
def pack_varint(v):o=bytearray();exec('b=v&0x7F;v>>=7;o.append(b|0x80 if v!=0 else b);if v==0:break;'*5);return bytes(o) # Compact
def read_varint(s):n,sh,br=0,0,0;exec('try:\n b=s.recv(1)\n if not b:return None\n v=b[0];n|=(v&0x7F)<<sh;sh+=7;br+=1\n if not v&0x80:raise StopIteration\n if br>5:return None\nexcept StopIteration:pass\nexcept socket.timeout:return None\nexcept OSError:return None;'*5);return n
def sanitize_motd(m):
    if not m:return ""
    t = "";
    if isinstance(m,dict):t="".join(p.get("text","")for p in m.get('extra',[]))or m.get("text","")
    elif isinstance(m,list):t="".join(p.get("text","")for p in m if isinstance(p,dict))
    else:t=str(m)
    return STRIP_FORMATTING.sub('',t).strip()
def ping_modern(ip,p=DEFAULT_PORT,t=0.4):
    log.debug(f"Modern ping: {ip}:{p} T:{t}s");start=time.monotonic();c=None
    try:
        c=socket.create_connection((ip,p),timeout=t);pv=765;pk=bytearray();pk+=pack_varint(0);pk+=pack_varint(pv);ib=ip.encode();pk+=pack_varint(len(ib));pk+=ib;pk+=struct.pack('>H',p);pk+=pack_varint(1);c.sendall(pack_varint(len(pk))+pk);c.sendall(pack_varint(1)+b'\x00');rl=read_varint(c)
        if rl is None or rl<=0:return None
        ri=read_varint(c)
        if ri!=0:log.warning(f"Modern ping {ip}:{p}: Bad PacketID {ri:#x}");return None
        jl=read_varint(c)
        if jl is None or jl<=0:return None
        jd=b'';br=jl
        while br>0:chk=c.recv(min(br,4096));br-=len(chk);jd+=chk;if not chk:return None
        try:r=json.loads(jd.decode())
        except Exception as e:log.warning(f"Modern ping {ip}:{p} JSON decode error: {e}");return None
        motd=sanitize_motd(r.get("description",""));pl=r.get("players",{});v=r.get("version",{}).get("name","");smp=pl.get("sample",[]);pn=[p.get("name","")for p in smp if isinstance(p,dict)and p.get("name")][:20];log.debug(f"Modern OK {ip}:{p} in {time.monotonic()-start:.3f}s");return{"motd":motd,"players_online":pl.get("online",0),"players_max":pl.get("max",0),"version":v or "unknown","player_names":pn}
    except(socket.timeout,ConnectionRefusedError,OSError)as e:log.debug(f"Modern ping {ip}:{p} Conn error: {e}");return None
    except Exception as e:log.error(f"Modern ping {ip}:{p} Error: {e}",exc_info=False);return None
    finally:
        if c:try:c.close()
              except:pass
def ping_legacy(ip,p=DEFAULT_PORT,t=0.4):
    log.debug(f"Legacy ping: {ip}:{p} T:{t}s");c=None
    try:c=socket.create_connection((ip,p),timeout=t);c.sendall(b'\xfe\x01');d=c.recv(1024);return d
    except(socket.timeout,OSError)as e:log.debug(f"Legacy ping {ip}:{p} Conn error: {e}");return None
    except Exception as e:log.error(f"Legacy ping {ip}:{p} Error: {e}",exc_info=False);return None
    finally:
        if c:try:c.close()
              except:pass
def parse_legacy_ping(r):
    if not r:return None
    try:
        if r.startswith(b'\xff'):parts=r[1:].decode('utf-16be','ignore').split('\xa7');return None # Kick
        elif b'\x00' in r:
            si=-1
            if r.startswith(b'\xff\x00'):si=r.find(b'\x00\x00\x00',1);si+=3 if si!=-1 else 0
            elif r.startswith(b'\xfe\x01\xfa'):si=3
            if si!=-1:parts=r[si:].decode('utf-16be','ignore').split('\x00');log.debug(f"Parsed legacy FE01");return{"motd":sanitize_motd(parts[3]),"players_online":int(parts[4]),"players_max":int(parts[5]),"version":parts[1]+"/"+parts[2],"player_names":[]}
        elif b'\xa7' in r:
            si=r.find(b'\xa7');
            if si!=-1:d=r[si+1:].decode('latin-1','ignore').split('§');log.debug(f"Parsed legacy §");return{"motd":sanitize_motd(d[0]),"players_online":int(d[1]),"players_max":int(d[2]),"version":"Legacy","player_names":[]}
    except Exception as e:log.warning(f"Legacy parse failed: {e} (Data: {r[:50]}...)")
    return None
def ping_server(ip,port=DEFAULT_PORT,timeout=0.4):
    m=ping_modern(ip,port,timeout=timeout);
    if m is not None:return m
    l=ping_legacy(ip,port,timeout=timeout);
    if l:p=parse_legacy_ping(l);return p
    return None