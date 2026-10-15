import time
import subprocess
import re
import json
from kafka import KafkaProducer

ROUTER_IP = "172.28.152.12"
INTERFACE_NAME = "FastEthernet1/1"
COMMUNITY = "public"
KAFKA_SERVER = "localhost:9092"
TOPIC = "snmp"
INTERVAL = 2

OID_IN = "1.3.6.1.2.1.2.2.1.10.2"   # ifInOctets
OID_OUT = "1.3.6.1.2.1.2.2.1.16.2"  # ifOutOctets

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_snmp_value(oid):
    cmd = ["snmpwalk", "-v2c", "-c", COMMUNITY, "-On", ROUTER_IP, oid]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode == 0:
        match = re.search(r'(?:Counter32|Counter64|INTEGER):\s*(\d+)', res.stdout)
        if match:
            return int(match.group(1))
    return 0

print("=================================")
print("Cisco SNMP Kafka Collector")
print("=================================")
print(f"Router       : {ROUTER_IP}")
print(f"Interface    : {INTERFACE_NAME}")
print(f"Kafka Server : {KAFKA_SERVER}")
print(f"Kafka Topic  : {TOPIC}")
print("=================================")

in_prev = get_snmp_value(OID_IN)
out_prev = get_snmp_value(OID_OUT)
print("Initial SNMP counters collected...")

while True:
    time.sleep(INTERVAL)
    in_curr = get_snmp_value(OID_IN)
    out_curr = get_snmp_value(OID_OUT)

    delta_in = (in_curr - in_prev) if in_curr >= in_prev else (in_curr - in_prev + 4294967296)
    delta_out = (out_curr - out_prev) if out_curr >= out_prev else (out_curr - out_prev + 4294967296)

    in_mbps = round((delta_in * 8) / (INTERVAL * 1_000_000), 4)
    out_mbps = round((delta_out * 8) / (INTERVAL * 1_000_000), 4)

    payload = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "router": ROUTER_IP,
        "interface": INTERFACE_NAME,
        "in_octets": in_curr,
        "out_octets": out_curr,
        "in_mbps": in_mbps,
        "out_mbps": out_mbps
    }

    future = producer.send(TOPIC, value=payload)
    metadata = future.get(timeout=10)

    print(f"IN: {in_mbps:.4f} Mbps | OUT: {out_mbps:.4f} Mbps | Kafka partition={metadata.partition} offset={metadata.offset}")

    in_prev = in_curr
    out_prev = out_curr
