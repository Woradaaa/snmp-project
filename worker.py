import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'snmp',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for msg in consumer:
    data = msg.value
    print(f"Timestamp  : {data.get('timestamp')}")
    print(f"Router     : {data.get('router')}")
    print(f"Interface  : {data.get('interface')}")
    print(f"IN Octets  : {data.get('in_octets')}")
    print(f"OUT Octets : {data.get('out_octets')}")
    print(f"IN Traffic : {data.get('in_mbps'):.4f} Mbps")
    print(f"OUT Traffic: {data.get('out_mbps'):.4f} Mbps")
    print(f"Partition  : {msg.partition}")
    print(f"Offset     : {msg.offset}")
    print("-----------------------------------")
