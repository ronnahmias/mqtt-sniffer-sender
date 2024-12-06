import paho.mqtt.client as mqtt
import json
import time
from datetime import datetime
import logging
import os


class MQTTSender:
    def __init__(self, broker="localhost", port=5678, log_file=None):
        self.broker = broker
        self.port = port
        self.log_file = log_file
        self.client = mqtt.Client()

        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.logger.info(f"Connected to broker {self.broker}:{self.port}")
        else:
            self.logger.error(f"Connection failed with code {rc}")

    def read_messages(self):
        """Read messages from log file"""
        try:
            with open(self.log_file, 'r') as f:
                messages = json.load(f)  # Load JSON array directly
            return sorted(messages, key=lambda x: x['timestamp'])
        except json.JSONDecodeError as e:
            self.logger.error(f"Error decoding JSON: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Error reading messages: {e}")
            return []

    def replay_messages(self, speed_factor=1.0):
        """Replay messages with original timing"""
        messages = self.read_messages()
        if not messages:
            self.logger.error("No messages found in log file")
            return

        self.client.on_connect = self.on_connect
        self.client.connect(self.broker, self.port)
        self.client.loop_start()

        try:
            total_messages = len(messages)
            self.logger.info(f"Starting replay of {total_messages} messages")
            prev_time = datetime.fromisoformat(messages[0]['timestamp'])

            for i, msg in enumerate(messages, 1):
                current_time = datetime.fromisoformat(msg['timestamp'])

                # Calculate delay
                delay = (current_time - prev_time).total_seconds() / \
                    speed_factor
                if delay > 0:
                    time.sleep(delay)

                # Publish message
                topic = msg['topic']
                payload = json.dumps(msg['payload']) if isinstance(
                    msg['payload'], dict) else msg['payload']

                self.client.publish(topic, payload)
                self.logger.info(
                    f"Published {i}/{total_messages} to {topic}: {payload}")

                prev_time = current_time

            self.logger.info(f"Replay completed: {
                             total_messages} messages sent")

        except KeyboardInterrupt:
            self.logger.info("Stopping message replay...")
        finally:
            self.client.loop_stop()
            self.client.disconnect()
            self.logger.info("Disconnected from broker")


def find_latest_log():
    """Find the most recent log file in the logs directory"""
    log_dir = "logs"
    if not os.path.exists(log_dir):
        return None

    log_files = [f for f in os.listdir(
        log_dir) if f.startswith("mqtt_messages_")]
    if not log_files:
        return None

    latest_log = max(log_files)
    return os.path.join(log_dir, latest_log)


if __name__ == "__main__":
    # Find the latest log file
    log_file = find_latest_log()
    if not log_file:
        print("No log files found in 'logs' directory")
        exit(1)

    print(f"Using log file: {log_file}")

    # Create and start the sender
    sender = MQTTSender(
        broker="localhost",
        port=5678,
        log_file=log_file
    )

    # Replay messages at normal speed (use speed_factor to adjust)
    # speed_factor > 1.0 makes it faster, < 1.0 makes it slower
    sender.replay_messages(speed_factor=1.0)
