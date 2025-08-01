#!/usr/bin/env python3
"""
Kafka Consumer Service - FIXED VERSION
Processes videos from Kafka queues with proper status updates
"""

import os
import sys
import json
import time
import asyncio
import threading
import requests
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Add project path
current_dir = os.getcwd()
sys.path.append(current_dir)
sys.path.append(os.path.join(current_dir, 'services'))

from services.transcript_extractor import YouTubeTranscriptExtractor
from services.langchain_analyzer import BabblIntelligenceAnalyzer

class KafkaVideoConsumer:
    """
    Kafka consumer that processes video jobs from priority queues
    Fixed version with proper status updates and data flow
    """
    
    def __init__(self):
        print("🚀 Initializing Kafka Video Consumer...")
        
        # Initialize processing components
        self.transcript_extractor = YouTubeTranscriptExtractor(cache_enabled=True)
        self.intelligence_analyzer = BabblIntelligenceAnalyzer()
        
        # Initialize infrastructure
        self.redis_client = self._init_redis()
        self.postgres_conn = self._init_postgres()
        self.kafka_producer = self._init_kafka_producer()
        
        # FastAPI endpoint for status updates
        self.fastapi_url = "http://localhost:8080"
        
        # Consumer configuration
        self.consumer_config = {
            'bootstrap_servers': ['localhost:9092'],
            'group_id': 'babbl-video-processors',
            'auto_offset_reset': 'latest',
            'enable_auto_commit': True,
            'value_deserializer': lambda m: json.loads(m.decode('utf-8'))
        }
        
        # Priority topics
        self.priority_topics = [
            'video-processing-urgent',
            'video-processing-high', 
            'video-processing-normal'
        ]
        
        # Metrics
        self.metrics = {
            'videos_processed': 0,
            'total_processing_time': 0,
            'sla_violations': 0,
            'cache_hits': 0,
            'errors': 0,
            'start_time': time.time()
        }
        
        print("✅ Kafka Consumer ready!")
    
    def _init_redis(self):
        """Initialize Redis connection"""
        try:
            import redis
            client = redis.Redis(host='localhost', port=6379, decode_responses=True)
            client.ping()
            print("✅ Redis connected")
            return client
        except Exception as e:
            print(f"⚠️ Redis connection failed: {e}")
            return None
    
    def _init_postgres(self):
        """Initialize PostgreSQL connection"""
        try:
            import psycopg2
            conn = psycopg2.connect(
                host='localhost', port=5433,
                database='babbl', user='user', password='pass'
            )
            print("✅ PostgreSQL connected")
            return conn
        except Exception as e:
            print(f"⚠️ PostgreSQL connection failed: {e}")
            return None
    
    def _init_kafka_producer(self):
        """Initialize Kafka producer for results"""
        try:
            from kafka import KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("✅ Kafka Producer connected")
            return producer
        except Exception as e:
            print(f"⚠️ Kafka Producer connection failed: {e}")
            return None
    
    def update_job_status(self, job_id: str, status: str, progress: str = None, error: str = None, result: dict = None):
        """Update job status in Redis and notify FastAPI - FIXED VERSION"""
        timestamp = datetime.now().isoformat()
        
        print(f"🔄 [Consumer] Updating job {job_id} status to: {status}")
        
        # Update Redis
        if self.redis_client:
            try:
                # Get existing job info
                existing_job = self.redis_client.get(f"job:{job_id}")
                job_info = {}
                
                if existing_job:
                    job_info = json.loads(existing_job)
                
                # Update with new status
                job_info.update({
                    "job_id": job_id,
                    "status": status,
                    "progress": progress,
                    "error": error,
                    "result": result,
                    "updated_at": timestamp
                })
                
                # Store in Redis with proper key
                self.redis_client.setex(
                    f"job:{job_id}",
                    3600,  # 1 hour TTL
                    json.dumps(job_info, default=str)
                )
                
                print(f"✅ [Consumer] Updated job {job_id} status to: {status} in Redis")
                
            except Exception as e:
                print(f"❌ [Consumer] Failed to update Redis: {e}")
        else:
            print(f"⚠️ [Consumer] Redis client not available")
    
    def process_video_job(self, job_data: dict) -> dict:
        """Process individual video job with proper status updates - FIXED"""
        start_time = time.time()
        
        # Extract job details
        job_id = job_data.get('job_id', 'unknown')
        video_url = job_data.get('video_url') or job_data.get('youtube_url')
        priority = job_data.get('priority', 'normal')
        queued_at = job_data.get('queued_at') or job_data.get('submitted_at')
        
        print(f"🎬 Processing job {job_id}: {priority} priority")
        
        if not video_url:
            error_msg = "No video URL found in job data"
            self.update_job_status(job_id, "failed", error=error_msg)
            raise ValueError(error_msg)
        
        try:
            # Update status to processing
            self.update_job_status(job_id, "processing", "Starting analysis...")
            
            # Extract video ID
            video_id = self._extract_video_id(video_url)
            
            # Check cache first
            cached_result = self._get_cached_result(video_id)
            if cached_result:
                self.metrics['cache_hits'] += 1
                print(f"📦 Cache hit for {video_id}")
                
                # Update status to completed with cached result
                self.update_job_status(job_id, "completed", result=cached_result)
                return cached_result
            
            # Extract transcript
            self.update_job_status(job_id, "processing", "Extracting transcript...")
            print("📥 Extracting transcript...")
            transcript_start = time.time()
            transcript_data = self.transcript_extractor.extract_from_url(video_url)
            transcript_time = time.time() - transcript_start
            
            # AI analysis
            self.update_job_status(job_id, "processing", "Running AI analysis...")
            print("🧠 Running AI analysis...")
            analysis_start = time.time()
            analysis_results = self.intelligence_analyzer.comprehensive_analysis(
                transcript_data['full_text']
            )
            analysis_time = time.time() - analysis_start
            
            # Store in ChromaDB
            self.update_job_status(job_id, "processing", "Storing in vector database...")
            self._store_in_chromadb(video_id, transcript_data, analysis_results)
            
            # Compile final results
            total_time = time.time() - start_time
            
            result = {
                "job_id": job_id,
                "video_id": video_id,
                "video_url": video_url,
                "priority": priority,
                "transcript_data": {
                    "segment_count": transcript_data.get('segment_count', 0),
                    "character_count": transcript_data.get('character_count', 0),
                    "full_text": transcript_data.get('full_text', '')[:500] + "..."  # Truncated for response
                },
                "analysis": {
                    "business_insights": analysis_results.business_insights,
                    "entity_analysis": analysis_results.entity_analysis,
                    "sentiment_summary": analysis_results.sentiment_summary,
                    "competitive_intelligence": analysis_results.competitive_intelligence,
                    "financial_implications": analysis_results.financial_implications
                },
                "processing_metrics": {
                    "total_time": total_time,
                    "transcript_time": transcript_time,
                    "analysis_time": analysis_time,
                    "timestamp": datetime.now().isoformat()
                }
            }
            
            # Store results
            self._store_video_result(result)
            self._cache_result(video_id, result)
            
            # Update status to completed - THIS IS THE KEY FIX
            self.update_job_status(job_id, "completed", result=result)
            
            # Update metrics
            self.metrics['videos_processed'] += 1
            self.metrics['total_processing_time'] += total_time
            
            print(f"✅ Job {job_id} completed in {total_time:.2f}s")
            return result
            
        except Exception as e:
            error_msg = str(e)
            print(f"❌ Job {job_id} failed: {error_msg}")
            
            # Update status to failed - THIS IS ALSO IMPORTANT
            self.update_job_status(job_id, "failed", error=error_msg)
            
            self.metrics['errors'] += 1
            raise e
    
    def _store_in_chromadb(self, video_id: str, transcript_data: dict, analysis_results):
        """Store video data in ChromaDB vector database"""
        try:
            from services.chromadb_integration import BabblVectorStore
            vector_store = BabblVectorStore()
            
            # Store transcript chunks
            full_text = transcript_data.get('full_text', '')
            if full_text:
                # Split into chunks for better search
                chunks = self._chunk_text(full_text, chunk_size=1000)
                
                for i, chunk in enumerate(chunks):
                    vector_store.add_document(
                        document=chunk,
                        metadata={
                            "video_id": video_id,
                            "chunk_index": i,
                            "content_type": "transcript",
                            "analysis_timestamp": datetime.now().isoformat()
                        },
                        collection="video_transcripts"
                    )
            
            # Store analysis results
            vector_store.add_document(
                document=analysis_results.business_insights,
                metadata={
                    "video_id": video_id,
                    "content_type": "business_insights",
                    "analysis_timestamp": datetime.now().isoformat()
                },
                collection="video_analysis"
            )
            
            print(f"✅ Stored video {video_id} in ChromaDB")
            
        except Exception as e:
            print(f"⚠️ ChromaDB storage failed: {e}")
    
    def _chunk_text(self, text: str, chunk_size: int = 1000) -> list:
        """Split text into chunks for vector storage"""
        words = text.split()
        chunks = []
        current_chunk = []
        current_length = 0
        
        for word in words:
            if current_length + len(word) > chunk_size and current_chunk:
                chunks.append(' '.join(current_chunk))
                current_chunk = [word]
                current_length = len(word)
            else:
                current_chunk.append(word)
                current_length += len(word) + 1  # +1 for space
        
        if current_chunk:
            chunks.append(' '.join(current_chunk))
        
        return chunks
    
    def consume_priority_queue(self, topic: str, consumer: KafkaConsumer):
        """Consume messages from specific priority queue"""
        priority = topic.split('-')[-1]
        print(f"🎯 Starting consumer for {topic} ({priority} priority)")
        
        try:
            for message in consumer:
                job_data = message.value
                print(f"📨 Received {priority} priority job: {job_data.get('job_id', 'unknown')}")
                
                try:
                    # Process the job
                    result = self.process_video_job(job_data)
                    print(f"✅ Successfully processed job {job_data.get('job_id')}")
                    
                except Exception as e:
                    print(f"❌ Failed to process job {job_data.get('job_id')}: {e}")
                
        except Exception as e:
            print(f"❌ Consumer error for {topic}: {e}")
    
    def start_consuming(self):
        """Start consuming from all priority queues"""
        print("🚀 Starting Kafka Video Consumer Service")
        print("="*60)
        
        # Create consumers for all priority topics
        consumers = self.create_priority_consumers()
        
        if not consumers:
            print("❌ No consumers created - exiting")
            return
        
        # Start consumer threads for each priority level
        threads = []
        
        for topic, consumer in consumers.items():
            thread = threading.Thread(
                target=self.consume_priority_queue,
                args=(topic, consumer),
                daemon=True
            )
            thread.start()
            threads.append(thread)
        
        print(f"✅ Started {len(threads)} consumer threads")
        
        try:
            # Keep main thread alive and show metrics
            while True:
                time.sleep(30)  # Update metrics every 30 seconds
                self._show_metrics()
                
        except KeyboardInterrupt:
            print("\n🛑 Shutting down consumers...")
            
            # Close all consumers
            for consumer in consumers.values():
                consumer.close()
            
            # Wait for threads to finish
            for thread in threads:
                thread.join(timeout=5)
            
            print("✅ Consumers shutdown complete")
    
    def create_priority_consumers(self):
        """Create consumers for each priority level"""
        consumers = {}
        
        for topic in self.priority_topics:
            try:
                consumer = KafkaConsumer(
                    topic,
                    **self.consumer_config
                )
                consumers[topic] = consumer
                print(f"✅ Consumer created for {topic}")
            except Exception as e:
                print(f"❌ Failed to create consumer for {topic}: {e}")
        
        return consumers
    
    def _extract_video_id(self, youtube_url: str) -> str:
        """Extract video ID from YouTube URL"""
        import re
        patterns = [
            r'(?:v=|\/)([0-9A-Za-z_-]{11}).*',
            r'(?:embed\/)([0-9A-Za-z_-]{11})',
            r'(?:youtu\.be\/)([0-9A-Za-z_-]{11})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, youtube_url)
            if match:
                return match.group(1)
        
        raise ValueError(f"Could not extract video ID from URL: {youtube_url}")
    
    def _get_cached_result(self, video_id: str):
        """Get cached result from Redis"""
        if not self.redis_client:
            return None
        
        try:
            cached = self.redis_client.get(f"video_result:{video_id}")
            return json.loads(cached) if cached else None
        except Exception:
            return None
    
    def _cache_result(self, video_id: str, result: dict):
        """Cache result in Redis"""
        if not self.redis_client:
            return
        
        try:
            self.redis_client.setex(
                f"video_result:{video_id}",
                3600,  # 1 hour TTL
                json.dumps(result, default=str)
            )
        except Exception as e:
            print(f"⚠️ Cache error: {e}")
    
    def _store_video_result(self, result: dict):
        """Store result in PostgreSQL"""
        if not self.postgres_conn:
            return
        
        try:
            cursor = self.postgres_conn.cursor()
            
            # Insert or update video
            cursor.execute("""
                INSERT INTO videos (video_id, video_url, processed_at, processing_time)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (video_id) DO UPDATE SET
                    processed_at = EXCLUDED.processed_at,
                    processing_time = EXCLUDED.processing_time
            """, (
                result["video_id"],
                result["video_url"],
                datetime.now(),
                result["processing_metrics"]["total_time"]
            ))
            
            # Insert analysis
            cursor.execute("""
                INSERT INTO video_analysis (video_id, business_insights, entity_analysis, 
                                          sentiment_summary, competitive_intelligence, financial_implications)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (video_id) DO UPDATE SET
                    business_insights = EXCLUDED.business_insights,
                    entity_analysis = EXCLUDED.entity_analysis,
                    sentiment_summary = EXCLUDED.sentiment_summary,
                    competitive_intelligence = EXCLUDED.competitive_intelligence,
                    financial_implications = EXCLUDED.financial_implications
            """, (
                result["video_id"],
                result["analysis"]["business_insights"],
                result["analysis"]["entity_analysis"],
                result["analysis"]["sentiment_summary"],
                result["analysis"]["competitive_intelligence"],
                result["analysis"]["financial_implications"]
            ))
            
            self.postgres_conn.commit()
            print(f"✅ Stored video {result['video_id']} in PostgreSQL")
            
        except Exception as e:
            print(f"⚠️ Database error: {e}")
            if self.postgres_conn:
                self.postgres_conn.rollback()
    
    def _show_metrics(self):
        """Show current processing metrics"""
        uptime = time.time() - self.metrics['start_time']
        
        print(f"\n📊 CONSUMER METRICS ({datetime.now().strftime('%H:%M:%S')})")
        print(f"   🎬 Videos processed: {self.metrics['videos_processed']}")
        
        if self.metrics['videos_processed'] > 0:
            avg_time = self.metrics['total_processing_time'] / self.metrics['videos_processed']
            cache_rate = self.metrics['cache_hits'] / self.metrics['videos_processed']
            throughput = self.metrics['videos_processed'] / max(uptime / 3600, 0.001)
            
            print(f"   ⏱️  Avg processing time: {avg_time:.2f}s")
            print(f"   📦 Cache hit rate: {cache_rate:.1%}")
            print(f"   🚀 Throughput: {throughput:.1f} videos/hour")
        
        print(f"   ⚠️  SLA violations: {self.metrics['sla_violations']}")
        print(f"   ❌ Errors: {self.metrics['errors']}")
        print(f"   ⏰ Uptime: {uptime / 60:.1f} minutes")

def main():
    """Main consumer service"""
    consumer = KafkaVideoConsumer()
    consumer.start_consuming()

if __name__ == "__main__":
    main()