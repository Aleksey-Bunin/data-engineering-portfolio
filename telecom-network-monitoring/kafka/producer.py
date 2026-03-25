"""
Network Events Producer for Testing
Generates realistic network events and sends to Kafka
"""

import json
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import time

# Event types
EVENT_TYPES = ['call_start', 'call_end', 'sms_sent', 'data_session', 'network_error']
REGIONS = ['North', 'South', 'East', 'West', 'Central']
ERROR_CODES = ['', '', '', '', 'E001', 'E002', 'E003']  # 70% no error

def generate_network_event():
    """Generate realistic network event"""
    event_type = random.choice(EVENT_TYPES)
    
    event = {
        'event_id': f"EVT{random.randint(100000, 999999)}",
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'event_type': event_type,
        'base_station_id': f"BS{random.randint(1, 100):03d}",
        'region': random.choice(REGIONS),
        'error_code': random.choice(ERROR_CODES),
        'call_duration': random.randint(10, 3600) if 'call' in event_type else 0,
        'data_usage_mb': round(random.uniform(0.1, 500.0), 2) if 'data' in event_type else 0.0
    }
    
    return event

def main():
    # Connect to Kafka
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print("🚀 Starting network events producer...")
    print("📡 Sending events to Kafka topic: network_events")
    
    events_sent = 0
    target_events = 100000
    
    try:
        while events_sent < target_events:
            event = generate_network_event()
            
            # Send to Kafka
            producer.send('network_events', value=event)
            
            events_sent += 1
            
            # Progress indicator
            if events_sent % 10000 == 0:
                print(f"✅ Sent {events_sent:,} events")
            
            # Small delay to simulate real-time (optional)
            # time.sleep(0.001)
        
        producer.flush()
        print(f"\n✅ Successfully sent {events_sent:,} events to Kafka!")
        
    except KeyboardInterrupt:
        print(f"\n⚠️ Stopped by user. Sent {events_sent:,} events.")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
