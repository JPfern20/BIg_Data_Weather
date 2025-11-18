import argparse
import json
import time
import random
import math
from datetime import datetime, timedelta
from typing import Dict, Any
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable


class StreamingDataProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

        self.producer_config = {
            'bootstrap_servers': bootstrap_servers,
            'value_serializer': lambda v: json.dumps(v).encode('utf-8'),  # JSON serialization
            'acks': 'all',  # ensure delivery confirmation
            'retries': 3,   # retry on failure
            'linger_ms': 5, # small batching
        }

        self.sensor_states = {}
        self.time_counter = 0
        self.base_time = datetime.utcnow()

        self.sensors = [
            {"id": "sensor_001", "location": "server_room_a", "type": "temperature", "unit": "celsius"},
            {"id": "sensor_002", "location": "server_room_b", "type": "temperature", "unit": "celsius"},
            {"id": "sensor_003", "location": "outdoor_north", "type": "temperature", "unit": "celsius"},
            {"id": "sensor_004", "location": "lab_1", "type": "humidity", "unit": "percent"},
            {"id": "sensor_005", "location": "lab_2", "type": "humidity", "unit": "percent"},
            {"id": "sensor_006", "location": "control_room", "type": "pressure", "unit": "hPa"},
            {"id": "sensor_007", "location": "factory_floor", "type": "pressure", "unit": "hPa"},
            {"id": "sensor_008", "location": "warehouse", "type": "temperature", "unit": "celsius"},
            {"id": "sensor_009", "location": "office_area", "type": "humidity", "unit": "percent"},
            {"id": "sensor_010", "location": "basement", "type": "pressure", "unit": "hPa"},
        ]

        self.metric_ranges = {
            "temperature": {"min": -10, "max": 45, "daily_amplitude": 8, "trend_range": (-0.5, 0.5)},
            "humidity": {"min": 20, "max": 95, "daily_amplitude": 15, "trend_range": (-0.2, 0.2)},
            "pressure": {"min": 980, "max": 1040, "daily_amplitude": 5, "trend_range": (-0.1, 0.1)},
        }

        try:
            self.producer = KafkaProducer(**self.producer_config)
            print(f"✅ Kafka producer initialized for {bootstrap_servers} on topic {topic}")
        except NoBrokersAvailable:
            print(f"❌ ERROR: No Kafka brokers available at {bootstrap_servers}")
            self.producer = None
        except Exception as e:
            print(f"❌ ERROR: Failed to initialize Kafka producer: {e}")
            self.producer = None

    def generate_sample_data(self) -> Dict[str, Any]:
        sensor = random.choice(self.sensors)
        sensor_id = sensor["id"]
        metric_type = sensor["type"]

        if sensor_id not in self.sensor_states:
            config = self.metric_ranges[metric_type]
            base_value = random.uniform(config["min"], config["max"])
            trend = random.uniform(*config["trend_range"])
            phase_offset = random.uniform(0, 2 * math.pi)
            self.sensor_states[sensor_id] = {
                "base_value": base_value,
                "trend": trend,
                "phase_offset": phase_offset,
                "last_value": base_value,
                "message_count": 0
            }

        state = self.sensor_states[sensor_id]
        current_time = self.base_time + timedelta(seconds=self.time_counter)
        self.time_counter += random.uniform(0.5, 2.0)

        config = self.metric_ranges[metric_type]
        hours_in_day = 24
        current_hour = current_time.hour + current_time.minute / 60
        daily_cycle = math.sin(2 * math.pi * current_hour / hours_in_day + state["phase_offset"])
        trend_effect = state["trend"] * (state["message_count"] / 100.0)
        noise = random.uniform(-config["daily_amplitude"] * 0.1, config["daily_amplitude"] * 0.1)

        raw_value = state["base_value"] + daily_cycle * config["daily_amplitude"] + trend_effect + noise
        bounded_value = max(config["min"], min(config["max"], raw_value))

        state["last_value"] = bounded_value
        state["message_count"] += 1

        if random.random() < 0.01:
            state["trend"] = random.uniform(*config["trend_range"])

        return {
            "timestamp": current_time.isoformat() + 'Z',
            "value": round(bounded_value, 2),
            "metric_type": metric_type,
            "sensor_id": sensor_id,
            "location": sensor["location"],
            "unit": sensor["unit"],
        }

    def send_message(self, data: Dict[str, Any]) -> bool:
        if not self.producer:
            print("❌ ERROR: Kafka producer not initialized")
            return False
        try:
            future = self.producer.send(self.topic, value=data)
            future.get(timeout=10)
            print(f"📤 Sent message: {data}")
            return True
        except KafkaError as e:
            print(f"❌ Kafka send error: {e}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return False

    def produce_stream(self, messages_per_second: float = 0.1, duration: int = None):
        print(f"🚀 Starting producer at {messages_per_second} msg/sec, duration: {duration or 'infinite'}")
        start_time = time.time()
        message_count = 0
        try:
            while True:
                if duration and (time.time() - start_time) >= duration:
                    print(f"⏹ Duration limit reached ({duration}s)")
                    break
                data = self.generate_sample_data()
                if self.send_message(data):
                    message_count += 1
                    if message_count % 10 == 0:
                        print(f"✅ Progress: {message_count} messages sent")
                time.sleep(1.0 / messages_per_second)
        except KeyboardInterrupt:
            print("\n⚠️ Producer interrupted by user")
        finally:
            self.close()
            print(f"Producer stopped. Total messages sent: {message_count}")

    def close(self):
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                self.producer.close()
                print("🔒 Kafka producer closed successfully")
            except Exception as e:
                print(f"❌ Error closing Kafka producer: {e}")


def parse_arguments():
    parser = argparse.ArgumentParser(description='Kafka Streaming Data Producer')
    parser.add_argument('--bootstrap-servers', type=str, default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--topic', type=str, default='streaming-data', help='Kafka topic')
    parser.add_argument('--rate', type=float, default=1.0, help='Messages per second')
    parser.add_argument('--duration', type=int, default=None, help='Run duration in seconds')
    parser.add_argument('--sensor-count', type=int, default=5, help='Number of simulated sensors')
    parser.add_argument('--data-type', choices=['temperature', 'humidity', 'pressure'], default=None,
                        help='Restrict to a specific metric type')
    return parser.parse_args()


def main():
    args = parse_arguments()
    producer = StreamingDataProducer(args.bootstrap_servers, args.topic)
    try:
        producer.produce_stream(messages_per_second=args.rate, duration=args.duration)
    except Exception as e:
        print(f"❌ Main execution error: {e}")
    finally:
        print("✅ Producer execution completed")


if __name__ == "__main__":
    main()
