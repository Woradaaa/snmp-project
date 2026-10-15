# 📡 Real-time SNMP Network Telemetry & Streaming Pipeline with Apache Kafka

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
```

---

## 📋 รายละเอียดองค์ประกอบระบบ (Component Specifications)

| องค์ประกอบ (Component) | เทคโนโลยี (Technology) | หน้าที่การทำงาน (Function Description) |
| :--- | :--- | :--- |
| **Network Router** | Cisco Router 7200 | อุปกรณ์เครือข่ายหลัก เปิดใช้งาน SNMP Read-Only Community (`public`) |
| **Telemetry Collector** | `collector.py` | ดึงค่า SNMP Octet Counter ทุกๆ 2 วินาที คำนวณเป็น Mbps และส่งเข้า Kafka |
| **Message Broker** | Apache Kafka | รับและจัดเก็บข้อมูลสตรีมมิ่งลงใน Topic `snmp` (Partition 0) |
| **Data Consumer** | `worker.py` | อ่านข้อมูลสตรีมมิ่งจาก Kafka Topic `snmp` และแสดงผลปริมาณ Traffic แยก Inbound/Outbound |
| **Traffic Generator** | `iperf3` (GNS3 IPTerm) | สร้าง Traffic จำลอง (Synthetic Traffic) ขนาด 5 Mbps เพื่อทดสอบระบบ |

---

## 📐 สูตรการคำนวณ Bandwidth (Mbps Calculation)

ระบบใช้การวัดผลต่างของไบต์ (Delta Bytes) จาก SNMP Counter32 ในช่วงเวลา 2 วินาที:

$$\text{Delta Bytes} = \text{Current Counter} - \text{Previous Counter}$$

*(กรณีเกิด 32-bit Counter Wrap-around: $\text{Delta Bytes} = \text{Delta Bytes} + 4294967296$)*

$$\text{Mbps} = \frac{\text{Delta Bytes} \times 8}{\text{Interval} \times 1000000}$$

### รายละเอียด SNMP OIDs ที่ใช้งาน
- **ifInOctets (Inbound Octets):** `1.3.6.1.2.1.2.2.1.10.2` (Interface FastEthernet1/1)
- **ifOutOctets (Outbound Octets):** `1.3.6.1.2.1.2.2.1.16.2` (Interface FastEthernet1/1)

---

## 🛠️ การติดตั้งและการใช้งาน (Installation & Deployment)

### 1. ติดตั้ง Dependencies
```bash
pip install -r requirements.txt
```

### 2. เริ่มการทำงานของ Kafka Consumer (Worker)
```bash
python worker.py
```

### 3. เริ่มการทำงานของ SNMP Collector (Producer)
```bash
python collector.py
```

### 4. ทดสอบสร้าง Traffic ด้วย iperf3
- **IPTerm 2 (Server):** `iperf3 -s`
- **IPTerm 1 (Client):** `iperf3 -c 172.16.1.10 -b 5M -t 600`

---

## 📁 โครงสร้างไฟล์ในโปรเจกต์ (Project Structure)
```text
snmp-project/
├── collector.py      # สคริปต์ดึง SNMP & ส่งเข้า Kafka Producer
├── worker.py         # สคริปต์อ่านค่าจาก Kafka Consumer
├── requirements.txt  # รายการ Python Packages
└── README.md         # เอกสารอธิบายโปรเจกต์
```