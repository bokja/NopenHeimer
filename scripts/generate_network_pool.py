import ipaddress
import os
import sys
import random # Import random module

# Add project root to sys.path if needed (helps find exclude.conf)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# sys.path.insert(0, project_root) # Usually not needed if run from root

# Programmatically generate network pools with exclusion

def parse_exclude_file(filepath):
    excluded_networks = []
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '-' in line:
                start_ip, end_ip = line.split('-')
                excluded_networks.append((ipaddress.IPv4Address(start_ip.strip()), ipaddress.IPv4Address(end_ip.strip())))
            else:
                excluded_networks.append(ipaddress.IPv4Network(line.strip(), strict=False))
    return excluded_networks

def is_excluded(network, excluded_networks):
    for ex in excluded_networks:
        if isinstance(ex, tuple):
            # Range exclusion
            if ipaddress.IPv4Address(network.network_address) >= ex[0] and ipaddress.IPv4Address(network.network_address) <= ex[1]:
                return True
        else:
            if network.overlaps(ex):
                return True
    return False

def generate_full_network_pool(exclude_file):
    excluded_networks = parse_exclude_file(exclude_file)
    network_pool = []
    for i in range(0, 256):
        for j in range(0, 16):  # Each /12 covers 16 /16 blocks
            net = ipaddress.IPv4Network(f"{i}.{j * 16}.0.0/12")
            if not is_excluded(net, excluded_networks):
                network_pool.append(str(net))
    return network_pool

if __name__ == "__main__":
    # Assume exclude.conf is in the project root relative to this script
    exclude_file_path = os.path.join(project_root, "exclude.conf")
    output_file_path = os.path.join(project_root, "network_pool.txt") # Save in root

    _default_network_pool = generate_full_network_pool(exclude_file_path)
    print(f"Total non-excluded /12 CIDR blocks generated: {len(_default_network_pool)}")

    # --- Shuffle the generated pool --- 
    print("Shuffling the network pool...")
    random.shuffle(_default_network_pool)
    print("Network pool shuffled.")
    # ---------------------------------

    # Save the SHUFFLED list to the file
    try:
        with open(output_file_path, "w") as f:
            f.write("# Generated Network Pool (/12 blocks, excluding overlaps from exclude.conf, SHUFFLED)\n")
            for net in _default_network_pool: # Now writing the shuffled list
                f.write(f"{net}\n")
        print(f"Shuffled network pool saved to {output_file_path}")
    except Exception as e:
        print(f"Error writing to output file {output_file_path}: {e}")
