# Cisco SNMP Kafka Telemetry Project

โปรเจกต์ระบบเฝ้าระวังและติดตามปริมาณการรับส่งข้อมูลเครือข่ายแบบ Real-time โดยทำการดึงข้อมูล Telemetry ผ่านโปรโตคอล SNMP จาก Cisco Router นำเข้าสู่ Apache Kafka Event Streaming Platform และประมวลผลผ่าน Python Worker

---

## 🏗️ สถาปัตยกรรมระบบ (System Architecture)

```text
+-----------------------+        SNMP (UDP 161)        +------------------------+
|  Cisco Router 7200    | <--------------------------- |  collector.py          |
|  (IP: 172.28.152.12)  |  ifInOctets / ifOutOctets    |  (SNMP Poller)         |
+-----------------------+                              +------------------------+
                                                                   |
                                                                   | JSON Payload
                                                                   v
                                                       +------------------------+
                                                       | Apache Kafka Broker    |
                                                       | (Topic: snmp)          |
                                                       +------------------------+
                                                                   |
                                                                   | Consume
                                                                   v
+-----------------------+        Console / Log         +------------------------+
| Terminal / Monitoring | <--------------------------- |  worker.py             |
| Real-time Dashboard   |   Formatted Traffic Stream   |  (Kafka Consumer)      |
+-----------------------+                              +------------------------+
