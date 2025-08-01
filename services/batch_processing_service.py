#!/usr/bin/env python3
"""
Batch Processing Service
Handles large-scale video processing for institutional clients
"""

import os
import sys
import json
import time
import asyncio
import concurrent.futures
from typing import List, Dict, Any
from datetime import datetime, timedelta
import uuid

# Add project path
current_dir = os.getcwd()
sys.path.append(current_dir)
sys.path.append(os.path.join(current_dir, 'services'))

from services.transcript_extractor import YouTubeTranscriptExtractor
from services.langchain_analyzer import BabblIntelligenceAnalyzer

class BabblBatchProcessor:
    """
    Large-scale batch processing for millions of videos daily
    Optimized for institutional client SLA requirements
    """
    
    def __init__(self, max_workers: int = 10):
        print("🎬 Initializing Babbl Batch Processor...")
        
        # Initialize processing components
        self.transcript_extractor = YouTubeTranscriptExtractor(cache_enabled=True)
        self.intelligence_analyzer = BabblIntelligenceAnalyzer()
        
        # Initialize infrastructure
        self.redis_client = self._init_redis()
        self.postgres_conn = self._init_postgres()
        self.kafka_producer = self._init_kafka_producer()
        self.vector_store = self._init_vector_store()
        
        # Batch processing configuration
        self.max_workers = max_workers
        self.batch_size = 50  # Process 50 videos per batch
        self.max_concurrent_batches = 5
        
        # Metrics tracking
        self.metrics = {
            'batches_processed': 0,
            'videos_processed': 0,
            'total_processing_time': 0,
            'cache_hits': 0,
            'errors': 0,
            'sla_violations': 0,
            'start_time': time.time()
        }
        
        print(f"✅ Batch processor ready (max {max_workers} workers)")
    
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
        """Initialize Kafka producer"""
        try:
            from kafka import KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                batch_size=32768,  # Larger batches for throughput
                linger_ms=100,     # Wait 100ms to batch more messages
                compression_type='gzip'
            )
            print("✅ Kafka Producer connected")
            return producer
        except Exception as e:
            print(f"⚠️ Kafka Producer connection failed: {e}")
            return None
    
    def _init_vector_store(self):
        """Initialize vector store"""
        try:
            # Import our vector store if available
            from chromadb_integration import BabblVectorStore
            vector_store = BabblVectorStore()
            print("✅ Vector store connected")
            return vector_store
        except Exception as e:
            print(f"⚠️ Vector store connection failed: {e}")
            return None
    
    async def process_video_batch(self, video_urls: List[str], batch_id: str = None,
                                  priority: str = "normal", callback_url: str = None) -> Dict[str, Any]:
        """
        Process batch of videos with parallel processing
        Core method for handling millions of videos daily
        """
        batch_id = batch_id or f"batch_{int(time.time())}_{str(uuid.uuid4())[:8]}"
        start_time = time.time()
        
        print(f"🎬 Processing batch {batch_id}: {len(video_urls)} videos ({priority} priority)")
        
        # Determine SLA deadline based on priority
        sla_deadlines = {
            'urgent': 15 * 60,    # 15 minutes
            'high': 30 * 60,      # 30 minutes
            'normal': 90 * 60,    # 90 minutes
            'low': 6 * 60 * 60    # 6 hours
        }
        sla_deadline = sla_deadlines.get(priority, 90 * 60)
        
        try:
            # Process videos in parallel using ThreadPoolExecutor
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                
                # Submit all video processing tasks
                future_to_url = {
                    executor.submit(self._process_single_video_sync, url, batch_id): url
                    for url in video_urls
                }
                
                # Collect results as they complete
                results = []
                completed = 0
                
                for future in concurrent.futures.as_completed(future_to_url):
                    url = future_to_url[future]
                    
                    try:
                        result = future.result()
                        results.append(result)
                        completed += 1
                        
                        # Progress update every 10 videos
                        if completed % 10 == 0:
                            elapsed = time.time() - start_time
                            rate = completed / elapsed
                            remaining = len(video_urls) - completed
                            eta = remaining / rate if rate > 0 else 0
                            
                            print(f"   📊 Progress: {completed}/{len(video_urls)} "
                                  f"({completed/len(video_urls)*100:.1f}%) "
                                  f"- ETA: {eta/60:.1f}min")
                        
                    except Exception as e:
                        print(f"❌ Error processing {url}: {e}")
                        results.append({
                            'video_url': url,
                            'error': str(e),
                            'batch_id': batch_id,
                            'timestamp': datetime.now().isoformat()
                        })
            
            # Calculate batch metrics
            total_time = time.time() - start_time
            successful_results = [r for r in results if 'error' not in r]
            failed_results = [r for r in results if 'error' in r]
            
            # Check SLA compliance
            sla_compliant = total_time < sla_deadline
            if not sla_compliant:
                self.metrics['sla_violations'] += 1
            
            batch_metrics = {
                'batch_id': batch_id,
                'total_videos': len(video_urls),
                'successful': len(successful_results),
                'failed': len(failed_results),
                'processing_time_seconds': total_time,
                'processing_time_minutes': total_time / 60,
                'throughput_videos_per_hour': len(video_urls) / (total_time / 3600),
                'throughput_videos_per_minute': len(video_urls) / (total_time / 60),
                'sla_deadline_minutes': sla_deadline / 60,
                'sla_compliant': sla_compliant,
                'priority': priority,
                'timestamp': datetime.now().isoformat(),
                'avg_processing_time_per_video': total_time / len(video_urls),
                'cache_hit_rate': self._calculate_cache_hit_rate(results),
                'error_rate': len(failed_results) / len(video_urls)
            }
            
            # Store batch results
            await self._store_batch_results(batch_id, results, batch_metrics)
            
            # Send batch completion events
            await self._send_batch_events(batch_metrics, callback_url)
            
            # Update global metrics
            self.metrics['batches_processed'] += 1
            self.metrics['videos_processed'] += len(successful_results)
            self.metrics['total_processing_time'] += total_time
            self.metrics['errors'] += len(failed_results)
            
            print(f"✅ Batch {batch_id} complete: "
                  f"{len(successful_results)}/{len(video_urls)} successful "
                  f"in {total_time/60:.1f}min "
                  f"({len(video_urls)/(total_time/3600):.0f} videos/hour)")
            
            return batch_metrics
            
        except Exception as e:
            error_metrics = {
                'batch_id': batch_id,
                'error': str(e),
                'timestamp': datetime.now().isoformat(),
                'processing_time_seconds': time.time() - start_time
            }
            
            print(f"❌ Batch {batch_id} failed: {e}")
            await self._store_batch_error(error_metrics)
            
            return error_metrics
    
    def _process_single_video_sync(self, video_url: str, batch_id: str) -> Dict[str, Any]:
        """
        Synchronous video processing for thread pool execution
        """
        start_time = time.time()
        
        try:
            # Extract video ID
            video_id = self._extract_video_id(video_url)
            
            # Check cache first
            cached_result = self._get_cached_result(video_id)
            if cached_result:
                self.metrics['cache_hits'] += 1
                cached_result['cache_hit'] = True
                cached_result['batch_id'] = batch_id
                return cached_result
            
            # Extract transcript
            transcript_data = self.transcript_extractor.extract_from_url(video_url)
            
            # AI analysis
            analysis_results = self.intelligence_analyzer.comprehensive_analysis(
                transcript_data['full_text']
            )
            
            # Compile results
            total_time = time.time() - start_time
            
            result = {
                'video_id': video_id,
                'video_url': video_url,
                'batch_id': batch_id,
                'cache_hit': False,
                'transcript_data': {
                    'segment_count': transcript_data['segment_count'],
                    'character_count': transcript_data['character_count']
                },
                'analysis_results': {
                    'business_insights': analysis_results.business_insights,
                    'entity_analysis': analysis_results.entity_analysis,
                    'sentiment_summary': analysis_results.sentiment_summary,
                    'competitive_intelligence': analysis_results.competitive_intelligence,
                    'financial_implications': analysis_results.financial_implications
                },
                'processing_metrics': {
                    'total_time': total_time,
                    'timestamp': datetime.now().isoformat()
                }
            }
            
            # Store and cache results (synchronous versions)
            self._store_video_result_sync(result)
            self._cache_result(video_id, result)
            
            # Store in vector database if available
            if self.vector_store:
                try:
                    # Add full transcript data for vector storage
                    result_for_vector = result.copy()
                    result_for_vector['transcript_data'] = transcript_data
                    self.vector_store.store_video_embeddings(video_id, result_for_vector)
                except Exception as e:
                    print(f"⚠️ Vector storage failed for {video_id}: {e}")
            
            return result
            
        except Exception as e:
            return {
                'video_url': video_url,
                'batch_id': batch_id,
                'error': str(e),
                'timestamp': datetime.now().isoformat(),
                'processing_time': time.time() - start_time
            }
    
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
    
    def _store_video_result_sync(self, result: dict):
        """Store video result in PostgreSQL (synchronous)"""
        if not self.postgres_conn:
            return
        
        try:
            cursor = self.postgres_conn.cursor()
            
            # Insert video
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
                ON CONFLICT (video_id) DO NOTHING
            """, (
                result["video_id"],
                result["analysis_results"]["business_insights"],
                result["analysis_results"]["entity_analysis"],
                result["analysis_results"]["sentiment_summary"],
                result["analysis_results"]["competitive_intelligence"],
                result["analysis_results"]["financial_implications"]
            ))
            
            self.postgres_conn.commit()
            
        except Exception as e:
            print(f"⚠️ Database error: {e}")
            self.postgres_conn.rollback()
    
    def _calculate_cache_hit_rate(self, results: List[Dict]) -> float:
        """Calculate cache hit rate for batch"""
        if not results:
            return 0.0
        
        cache_hits = sum(1 for r in results if r.get('cache_hit', False))
        return cache_hits / len(results)
    
    async def _store_batch_results(self, batch_id: str, results: List[Dict], metrics: Dict):
        """Store batch processing results"""
        
        if not self.postgres_conn:
            return
        
        try:
            cursor = self.postgres_conn.cursor()
            
            # Store batch metadata
            cursor.execute("""
                INSERT INTO batch_processing (
                    batch_id, video_count, successful_count, failed_count,
                    processing_time, throughput, sla_compliance_rate, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                batch_id,
                metrics['total_videos'],
                metrics['successful'],
                metrics['failed'],
                metrics['processing_time_seconds'],
                metrics['throughput_videos_per_hour'],
                1.0 if metrics['sla_compliant'] else 0.0,
                datetime.now()
            ))
            
            self.postgres_conn.commit()
            
        except Exception as e:
            print(f"⚠️ Batch storage error: {e}")
            self.postgres_conn.rollback()
    
    async def _send_batch_events(self, metrics: Dict, callback_url: str = None):
        """Send batch completion events"""
        
        # Send to Kafka
        if self.kafka_producer:
            try:
                event = {
                    'event_type': 'batch_completed',
                    'batch_id': metrics['batch_id'],
                    'metrics': metrics,
                    'timestamp': datetime.now().isoformat()
                }
                
                self.kafka_producer.send('batch-events', event)
                
            except Exception as e:
                print(f"⚠️ Kafka event error: {e}")
        
        # Send webhook notification if callback URL provided
        if callback_url:
            try:
                import requests
                requests.post(callback_url, json=metrics, timeout=10)
            except Exception as e:
                print(f"⚠️ Webhook error: {e}")
    
    async def _store_batch_error(self, error_metrics: Dict):
        """Store batch error"""
        if not self.postgres_conn:
            return
        
        try:
            cursor = self.postgres_conn.cursor()
            
            cursor.execute("""
                INSERT INTO processing_errors (video_id, video_url, error_message, error_timestamp, processing_time)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                error_metrics['batch_id'],
                'batch_processing',
                error_metrics['error'],
                datetime.now(),
                error_metrics['processing_time_seconds']
            ))
            
            self.postgres_conn.commit()
            
        except Exception as e:
            print(f"⚠️ Error storage failed: {e}")
    
    def get_batch_metrics(self) -> Dict[str, Any]:
        """Get current batch processing metrics"""
        uptime = time.time() - self.metrics['start_time']
        
        return {
            'batches_processed': self.metrics['batches_processed'],
            'videos_processed': self.metrics['videos_processed'],
            'total_processing_time': self.metrics['total_processing_time'],
            'average_batch_time': self.metrics['total_processing_time'] / max(self.metrics['batches_processed'], 1),
            'average_video_time': self.metrics['total_processing_time'] / max(self.metrics['videos_processed'], 1),
            'cache_hit_rate': self.metrics['cache_hits'] / max(self.metrics['videos_processed'], 1),
            'error_rate': self.metrics['errors'] / max(self.metrics['videos_processed'] + self.metrics['errors'], 1),
            'sla_violation_rate': self.metrics['sla_violations'] / max(self.metrics['batches_processed'], 1),
            'throughput_videos_per_hour': self.metrics['videos_processed'] / max(uptime / 3600, 0.001),
            'uptime_hours': uptime / 3600
        }

async def test_batch_processing():
    """Test batch processing functionality"""
    print("🧪 Testing Batch Processing")
    print("="*50)
    
    # Initialize batch processor
    processor = BabblBatchProcessor(max_workers=5)
    
    # Test with small batch
    test_videos = [
        "https://www.youtube.com/watch?v=0X0Jm8QValY",  # Your working video
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ"   # Another test video
    ]
    
    print(f"🎬 Processing test batch of {len(test_videos)} videos...")
    
    # Process batch
    results = await processor.process_video_batch(
        video_urls=test_videos,
        batch_id="test_batch_001",
        priority="high"
    )
    
    print(f"\n📊 Batch Results:")
    for key, value in results.items():
        if isinstance(value, float):
            print(f"   • {key}: {value:.2f}")
        else:
            print(f"   • {key}: {value}")
    
    # Show processor metrics
    metrics = processor.get_batch_metrics()
    print(f"\n📈 Processor Metrics:")
    for key, value in metrics.items():
        if isinstance(value, float):
            print(f"   • {key}: {value:.2f}")
        else:
            print(f"   • {key}: {value}")

if __name__ == "__main__":
    asyncio.run(test_batch_processing())