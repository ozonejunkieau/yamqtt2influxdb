This file is a quick hack at exporting data from MQTT to InfluxDB. It's not likely to be immediately useful to others, but may help others?

The data is fed into MQTT from the [ESP32-BLE2MQTT project](https://github.com/shmuelzon/esp32-ble2mqtt). I'm using a fork that encodes all the info from the ATC1441 firmware in a single notification, making it easier to get message updates and avoid race conditions.
