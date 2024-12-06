import paho.mqtt.client as mqtt
import json
import logging
from datetime import datetime


class TemperatureMonitor:
    def __init__(self, broker="localhost", port=5678):
        # Setup MQTT client
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.broker = broker
        self.port = port

        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.logger.info(f"Connected to MQTT broker at {
                             self.broker}:{self.port}")
            # Subscribe to temperature topic
            self.client.subscribe("sensors/readings")
            self.logger.info("Subscribed to sensors/readings")
        else:
            self.logger.error(f"Connection failed with code {rc}")

    def on_message(self, client, userdata, msg):
        try:
            # Parse the message payload
            payload = json.loads(msg.payload.decode())

            # Extract temperature and timestamp
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            temperature = payload.get('value')
            sensor_id = payload.get('sensor_id')

            if temperature is not None:
                print(f"[{timestamp}] Temperature from {
                      sensor_id}: {temperature}°C")
            else:
                self.logger.warning(
                    f"Received message without temperature value: {payload}")

        except json.JSONDecodeError:
            self.logger.error("Failed to decode JSON message")
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")

    def start(self):
        try:
            # Connect to broker
            self.logger.info(f"Connecting to broker {self.broker}:{self.port}")
            self.client.connect(self.broker, self.port)

            # Start the loop
            self.client.loop_forever()

        except KeyboardInterrupt:
            self.logger.info("Stopping temperature monitor...")
            self.client.disconnect()
            self.logger.info("Disconnected from broker")
        except Exception as e:
            self.logger.error(f"Error: {e}")


if __name__ == "__main__":
    # Create and start the monitor
    monitor = TemperatureMonitor(
        broker="localhost",  # Change this to your broker IP if needed
        port=5678           # Change this to your broker port
    )
    monitor.start()
