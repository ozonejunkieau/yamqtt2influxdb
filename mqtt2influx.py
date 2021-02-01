#!/usr/bin/python3
from dataclasses import dataclass, field
from datetime import datetime

import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient

from config import INFLUXDB_DB_NAME, INFLUX_DB_HOST, INFLUX_DB_PORT, INFLUX_DB_USERNAME, INFLUX_DB_PASSWORD
from config import MQTT_USERNAME, MQTT_PASSWORD, MQTT_HOST
from config import REGION_TAG, MAC_LOOKUP

INFLUX_BATCH_SIZE = 64 # Number of messages to await before submitting to influxdb

known_devices = {}

influx_messages = []

influx_client = InfluxDBClient(INFLUX_DB_HOST, INFLUX_DB_PORT, INFLUX_DB_USERNAME, INFLUX_DB_PASSWORD, INFLUXDB_DB_NAME)

@dataclass
class ATC1441:
    mac: str
    name: str
    rssi: dict = field(default_factory=dict)
    last_message_count: dict = field(default_factory=dict)

    def get_last_message_count(self):
        if len(self.last_message_count.keys()) == 0:
            return -1

        return max([v for _, v in self.last_message_count.items()])

 
receivers_list = ["BLE2MQTT-2963","BLE2MQTT-AE6B"]



def update_subscribers(client):
    sub_list = []

    # Build type susbscription
    for r in receivers_list:
        sub_list.append((f"{r}/+/Type",0))
        sub_list.append((f"{r}/Uptime",0))
        sub_list.append((f"{r}/FreeMemory",0))

    # Build device subscriptions
    for r in receivers_list:
        for _, d in known_devices.items():
            sub_list.append((f"{r}/{d.mac.lower()}/Summary",0))
            sub_list.append((f"{r}/{d.mac.lower()}/RSSI",0))

    client.subscribe(sub_list)

def topic_to_mac(topic):
    return topic[14:31].upper()

def topic_to_receiver(topic):
    return topic[0:13].upper()

def topic_to_mac_recv(topic):
    return topic_to_mac(topic), topic_to_receiver(topic)
    
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    update_subscribers(client)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):

    msg_time = datetime.utcnow().timestamp() * 1000

    if msg.topic.startswith("BLE2MQTT") and msg.topic.endswith("Type") and msg.payload == b'ATC1441':
        # a ATC1441 type notifier
        mac, recv = topic_to_mac_recv(msg.topic)
        if mac not in known_devices.keys():
            if mac in MAC_LOOKUP.keys():
                name = MAC_LOOKUP[mac]
            else:
                name = "UNKNOWN"
            known_devices[mac] = ATC1441(mac=mac, name=name)
            print(f"New ATC1441 found: {mac} ({len(known_devices.keys())})")
            update_subscribers(client)

    elif msg.topic.startswith("BLE2MQTT") and msg.topic.endswith("Uptime"):
        recv = topic_to_receiver(msg.topic)
        uptime_secs = int(msg.payload)

        this_message = {
            "measurement": "receivers",
            "timestamp": msg_time,
            "tags": {
                "region": REGION_TAG,
                "receiver": recv,
            },
            "fields": {
                "uptime": int(uptime_secs),
            }
    	}

        influx_messages.append(this_message)

    elif msg.topic.startswith("BLE2MQTT") and msg.topic.endswith("FreeMemory"):
        recv = topic_to_receiver(msg.topic)
        free_memory = int(msg.payload)

        this_message = {
            "measurement": "receivers",
            "timestamp": msg_time,
            "tags": {
                "region": REGION_TAG,
                "receiver": recv,
            },
            "fields": {
                "free_memory": int(free_memory),
            }
    	}

        influx_messages.append(this_message)


    elif msg.topic.startswith("BLE2MQTT") and msg.topic.endswith("RSSI"):
        mac, recv = topic_to_mac_recv(msg.topic)
        rssi = int(msg.payload)
        known_devices[mac].rssi[recv] = rssi
        #print(known_devices[mac])

        this_message = {
            "measurement": "sensors",
            "timestamp": msg_time,
            "tags": {
                "region": REGION_TAG,
                "location": f"{known_devices[mac].name}",
                "MAC": f"{mac}",
                "receiver": recv,
            },
            "fields": {
                "rssi": int(rssi),
            }
    	}

        influx_messages.append(this_message)

    elif msg.topic.startswith("BLE2MQTT") and msg.topic.endswith("Summary"):
        mac, recv = topic_to_mac_recv(msg.topic)
        _, msg_counter, temp, humid, batt_pc, batt_v = msg.payload.split(b"|")

        msg_counter = int(msg_counter)
        temp = float(temp)
        humid = int(humid)
        batt_pc = int(batt_pc)
        batt_v = float(batt_v)

        previous_message_count = known_devices[mac].get_last_message_count()
        known_devices[mac].last_message_count[recv] = msg_counter

        if msg_counter > previous_message_count:
            this_message = {
                "measurement": "environment",
                "timestamp": msg_time,
                "tags": {
                    "region": REGION_TAG,
                    "location": f"{known_devices[mac].name}",
                    "MAC": f"{mac}",
                },
                "fields": {
                    "temperature": float(temp),
                    "humidity": float(humid),
                    "batt_percent": float(batt_pc),
                    "batt_voltage": float(batt_v),
                }
    	    }

            influx_messages.append(this_message)

    else:
        # Not a handled message, print as debug.
        print(msg.topic+" "+str(msg.payload))

    # Output messages if a large enough batch has ocurred.
    if len(influx_messages) > INFLUX_BATCH_SIZE:
        #print("Writing to Influx...")
        try:
            influx_client.write_points(influx_messages)
            influx_messages.clear()
        except:
            pass


    
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

client.connect(MQTT_HOST, 1883, 60)

client.loop_forever()
