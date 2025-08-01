#!/usr/bin/env python3
"""
Independent Kafka Consumer Service for Babbl Labs
Runs separately from FastAPI - processes jobs from Kafka queues
"""

import os
import sys
import signal
import time
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent))

try:
    from services.kafka_consumer_service import KafkaVideoConsumer
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("💡 Make sure you're running from the project root directory")
    print("💡 Check that services/kafka_consumer_service.py exists")
    sys.exit(1)

def signal_handler(signum, frame):
    """Handle graceful shutdown"""
    print(f"\n🛑 Received signal {signum}")
    print("🔄 Shutting down consumer gracefully...")
    sys.exit(0)

def main():
    """Main consumer service - runs independently"""
    
    print("🚀 BABBL LABS KAFKA CONSUMER SERVICE")
    print("="*50)
    print("📋 Independent video processing service")
    print("🔗 Connects to: Kafka, Redis, PostgreSQL, ChromaDB")
    print("⚡ Processes videos from Kafka topics")
    print("📊 Updates job status in Redis for FastAPI")
    print("="*50)
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Initialize consumer
        print("🔧 Initializing Kafka Video Consumer...")
        consumer = KafkaVideoConsumer()
        
        print("✅ Consumer initialized successfully!")
        print("\n🎯 Listening for video jobs on Kafka topics:")
        print("   📺 video-processing-urgent")
        print("   📺 video-processing-high") 
        print("   📺 video-processing-normal")
        print("\n🔄 Consumer is running... (Ctrl+C to stop)")
        print("-" * 50)
        
        # Start consuming - this runs forever
        consumer.start_consuming()
        
    except KeyboardInterrupt:
        print("\n🛑 Consumer stopped by user")
    except Exception as e:
        print(f"❌ Consumer failed: {e}")
        sys.exit(1)
    finally:
        print("🏁 Consumer service shutdown complete")

if __name__ == "__main__":
    main()