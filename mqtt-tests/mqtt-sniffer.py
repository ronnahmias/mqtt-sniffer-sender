import paho.mqtt.client as mqtt
import json
import time
from datetime import datetime
import logging
import os


class MQTTSniffer:
    def __init__(self, broker="localhost", port=5678, log_file="mqtt_messages.json"):
        self.broker = broker
        self.port = port
        self.log_file = log_file
        self.client = mqtt.Client()
        self.messages = []  # Store messages in memory

        # Set up callbacks
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

        # Ensure the log directory exists
        os.makedirs(os.path.dirname(log_file) if os.path.dirname(
            log_file) else '.', exist_ok=True)

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.logger.info(f"Connected to broker {self.broker}:{self.port}")

            topics = [
                ("sensors/readings", 0),
            ]
            client.subscribe(topics)
        else:
            self.logger.error(f"Connection failed with code {rc}")

    def on_message(self, client, userdata, msg):
        try:
            # Try to decode JSON payload
            try:
                payload = json.loads(msg.payload.decode())
            except json.JSONDecodeError:
                payload = msg.payload.decode()

            # Create message record
            message_record = {
                "timestamp": datetime.now().isoformat(),
                "topic": msg.topic,
                "payload": payload,
            }

            # Add to messages array
            self.messages.append(message_record)

            # Save to file
            self.save_messages()

            # Log to console
            self.logger.info(f"Topic: {msg.topic} | Payload: {payload}")

        except Exception as e:
            self.logger.error(f"Error processing message: {e}")

    def save_messages(self):
        """Save all messages to file in JSON format"""
        try:
            with open(self.log_file, 'w') as f:
                json.dump(self.messages, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving messages: {e}")

    def start(self):
        try:
            # Connect to broker
            self.logger.info(f"Connecting to broker {self.broker}:{self.port}")
            self.client.connect(self.broker, self.port)

            # Start the loop
            self.client.loop_forever()

        except KeyboardInterrupt:
            self.logger.info("Stopping MQTT sniffer...")
            self.client.disconnect()
            self.save_messages()  # Save final version before exit
            self.logger.info("Disconnected from broker")
        except Exception as e:
            self.logger.error(f"Error: {e}")


if __name__ == "__main__":
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Create and start the sniffer
    sniffer = MQTTSniffer(
        broker="localhost",  # Change this to your broker IP if needed
        port=5678,          # Change this to your broker port
        log_file=f"logs/mqtt_messages_{timestamp}.json"
    )
    sniffer.start()
