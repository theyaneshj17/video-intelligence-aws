#!/usr/bin/env python3
"""
Independent FastAPI Producer Service for Babbl Labs
Runs separately from Kafka Consumer - only handles web API and job submission
"""

import os
import sys
import asyncio
import time
import uuid
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

# Add project root to path
sys.path.append(str(Path(__file__).parent))

# Import only what FastAPI needs (no consumer imports)
try:
    from kafka import KafkaProducer
    import redis
    import psycopg2
except ImportError as e:
    print(f"⚠️ Import error: {e}")
    print("💡 Make sure you have: kafka-python, redis, psycopg2-binary installed")

# Globals for FastAPI services only
kafka_producer: Optional[KafkaProducer] = None
redis_client: Optional[redis.Redis] = None
start_time = time.time()

@asynccontextmanager
async def lifespan(app: FastAPI):
    global kafka_producer, redis_client

    print("🚀 [FastAPI Producer] Initializing services...")

    try:
        # Initialize Kafka Producer
        kafka_producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda v: v.encode('utf-8') if v else None,
            batch_size=16384,
            linger_ms=10,
            compression_type='gzip',
            retries=3
        )
        print("✅ Kafka Producer initialized")

        # Initialize Redis
        redis_client = redis.Redis(
            host='localhost', port=6379, decode_responses=True, max_connections=20
        )
        redis_client.ping()
        print("✅ Redis client initialized")

        print("✅ FastAPI Producer ready! Consumer runs separately.")
        
    except Exception as e:
        print(f"❌ Startup failed: {e}")
        print("🎭 Running in limited mode")

    yield  # FastAPI serves endpoints

app = FastAPI(
    title="🎬 Babbl Labs Video Intelligence API - Producer",
    description="Independent FastAPI producer - submits jobs to Kafka queue",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request/Response Models
class VideoAnalysisRequest(BaseModel):
    youtube_url: str
    priority: Optional[str] = "normal"
    callback_url: Optional[str] = None
    client_id: Optional[str] = None

class VideoAnalysisResponse(BaseModel):
    job_id: str
    video_id: str
    youtube_url: str
    status: str
    estimated_completion: str
    queue_position: Optional[int] = None
    message: str
    timestamp: str

class JobStatusResponse(BaseModel):
    job_id: str
    video_id: str
    status: str
    progress: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    processing_time: Optional[float] = None
    error: Optional[str] = None
    timestamp: str

@app.get("/", response_class=HTMLResponse)
async def root():
    """Landing page"""
    try:
        with open("index.html", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return """
        <h1>🎬 Babbl Labs API - Producer</h1>
        <p>Independent FastAPI Producer Service</p>
        <p><strong>Consumer Status:</strong> Run separately in another terminal</p>
        <p><a href="/docs">API Documentation</a></p>
        <p><a href="/health">Health Check</a></p>
        """

@app.post("/analyze", response_model=VideoAnalysisResponse)
async def analyze_video_async(request: VideoAnalysisRequest):
    """
    Submit video for asynchronous analysis via Kafka queue
    PRODUCER ONLY - sends job to Kafka, consumer processes independently
    """
    
    if not kafka_producer:
        raise HTTPException(
            status_code=503,
            detail="Kafka producer unavailable. Cannot submit jobs."
        )
    
    try:
        # Validate YouTube URL
        if "youtube.com/watch" not in request.youtube_url and "youtu.be/" not in request.youtube_url:
            raise HTTPException(
                status_code=400,
                detail="Invalid YouTube URL. Please provide a valid youtube.com or youtu.be link."
            )
        
        # Generate unique job ID
        job_id = str(uuid.uuid4())
        video_id = extract_video_id(request.youtube_url)
        
        # Create job message for Kafka
        job_message = {
            "job_id": job_id,
            "video_id": video_id,
            "video_url": request.youtube_url,
            "youtube_url": request.youtube_url,
            "priority": request.priority,
            "callback_url": request.callback_url,
            "client_id": request.client_id,
            "queued_at": datetime.now().isoformat(),
            "status": "queued"
        }
        
        # Send to appropriate Kafka topic
        topic = get_kafka_topic_for_priority(request.priority)
        
        print(f"📤 [Producer] Sending video {video_id} to Kafka topic: {topic}")
        
        # Send to Kafka queue
        kafka_producer.send(
            topic,
            key=job_id,
            value=job_message
        )
        kafka_producer.flush()  # Ensure message is sent
        
        # Store initial job info in Redis
        await store_job_info(job_id, job_message)
        
        # Estimate completion time
        estimated_completion = estimate_completion_time(request.priority)
        
        response = VideoAnalysisResponse(
            job_id=job_id,
            video_id=video_id,
            youtube_url=request.youtube_url,
            status="queued",
            estimated_completion=estimated_completion,
            queue_position=1,  # Simplified
            message=f"Video analysis job submitted to Kafka. Consumer will process independently.",
            timestamp=datetime.now().isoformat()
        )
        
        print(f"✅ [Producer] Job {job_id} queued for video {video_id}")
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ [Producer] Job submission error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to submit job: {str(e)}"
        )

@app.get("/status/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """
    Get job status from Redis (updated by independent consumer)
    PRODUCER READS - consumer writes status updates
    """
    
    try:
        # Get job info from Redis (written by consumer)
        job_info = await get_job_info(job_id)
        
        if not job_info:
            raise HTTPException(
                status_code=404,
                detail=f"Job {job_id} not found"
            )
        
        status = job_info.get("status", "unknown")
        video_id = job_info.get("video_id", "")
        
        # Handle completed jobs
        if status == "completed":
            result = job_info.get("result")
            processing_time = None
            
            if result and result.get("processing_metrics"):
                processing_time = result["processing_metrics"].get("total_time")
            
            return JobStatusResponse(
                job_id=job_id,
                video_id=video_id,
                status="completed",
                result=result,
                processing_time=processing_time,
                timestamp=datetime.now().isoformat()
            )
        
        # Handle failed jobs
        elif status == "failed":
            return JobStatusResponse(
                job_id=job_id,
                video_id=video_id,
                status="failed",
                error=job_info.get("error", "Unknown error"),
                timestamp=datetime.now().isoformat()
            )
        
        # Handle processing/queued jobs
        else:
            return JobStatusResponse(
                job_id=job_id,
                video_id=video_id,
                status=status,
                progress=job_info.get("progress"),
                timestamp=datetime.now().isoformat()
            )
            
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ [Producer] Status check error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get job status: {str(e)}"
        )


# Add these missing endpoints to your FastAPI producer script

@app.get("/queue/status")
async def get_queue_status():
    """Get current Kafka queue status - FIXED"""
    
    try:
        queue_stats = {
            "normal_priority": 0,
            "high_priority": 0,
            "urgent": 0,
            "active_jobs": 0,
            "consumer_status": "running independently"
        }
        
        # Count active jobs from Redis
        if redis_client:
            try:
                keys = redis_client.keys("job:*")
                active_count = 0
                priority_counts = {"normal": 0, "high": 0, "urgent": 0}
                
                for key in keys:
                    job_data = redis_client.get(key)
                    if job_data:
                        job_info = json.loads(job_data)
                        status = job_info.get("status", "")
                        priority = job_info.get("priority", "normal")
                        
                        if status in ["queued", "processing"]:
                            active_count += 1
                            if priority in priority_counts:
                                priority_counts[priority] += 1
                
                queue_stats.update({
                    "active_jobs": active_count,
                    "normal_priority": priority_counts["normal"],
                    "high_priority": priority_counts["high"],
                    "urgent": priority_counts["urgent"]
                })
                
            except Exception as e:
                print(f"⚠️ Error counting active jobs: {e}")
        
        return queue_stats
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get queue status: {str(e)}"
        )

@app.get("/jobs/active")
async def get_active_jobs():
    """Get list of active jobs from Redis - FIXED"""
    
    try:
        active_jobs = {}
        
        if redis_client:
            keys = redis_client.keys("job:*")
            
            for key in keys:
                try:
                    job_data = redis_client.get(key)
                    if job_data:
                        job_info = json.loads(job_data)
                        job_id = job_info.get("job_id", key.replace("job:", ""))
                        status = job_info.get("status", "unknown")
                        
                        # Include all jobs (not just active ones for debugging)
                        active_jobs[job_id] = {
                            "video_id": job_info.get("video_id", ""),
                            "status": status,
                            "submitted_at": job_info.get("queued_at", job_info.get("submitted_at", "")),
                            "priority": job_info.get("priority", "normal"),
                            "updated_at": job_info.get("updated_at", "")
                        }
                        
                except Exception as e:
                    print(f"⚠️ Error processing job {key}: {e}")
                    continue
        
        return {
            "total_active_jobs": len(active_jobs),
            "jobs": active_jobs
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get active jobs: {str(e)}"
        )

# Also update your health check to be more comprehensive
@app.get("/health")
async def health_check():
    """Comprehensive health check for producer services"""
    
    uptime = time.time() - start_time
    services = {}
    overall_status = "healthy"
    
    # Check Kafka Producer
    if kafka_producer:
        try:
            kafka_producer.send('health-check', {"test": "connection"})
            kafka_producer.flush()
            services["kafka_producer"] = "healthy"
        except Exception as e:
            services["kafka_producer"] = f"unhealthy: {str(e)}"
            overall_status = "degraded"
    else:
        services["kafka_producer"] = "unavailable"
        overall_status = "degraded"
    
    # Check Redis
    if redis_client:
        try:
            redis_client.ping()
            # Count total jobs in Redis
            job_count = len(redis_client.keys("job:*"))
            services["redis"] = f"healthy ({job_count} jobs stored)"
        except Exception as e:
            services["redis"] = f"unhealthy: {str(e)}"
            overall_status = "degraded"
    else:
        services["redis"] = "unavailable"
        overall_status = "degraded"
    
    # Check PostgreSQL connection
    try:
        import psycopg2
        conn = psycopg2.connect(
            host='localhost', port=5433,
            database='babbl', user='user', password='pass'
        )
        conn.close()
        services["postgresql"] = "healthy"
    except Exception as e:
        services["postgresql"] = f"unavailable: {str(e)}"
        if overall_status == "healthy":
            overall_status = "degraded"
    
    # Check ChromaDB
    try:
        import chromadb
        client = chromadb.HttpClient(host='localhost', port=8001)
        client.heartbeat()
        services["chromadb"] = "healthy"
    except Exception as e:
        services["chromadb"] = f"unavailable: {str(e)}"
        if overall_status == "healthy":
            overall_status = "degraded"
    
    # Consumer status (independent process)
    services["kafka_consumer"] = "independent process - check consumer terminal"
    
    return {
        "status": overall_status,
        "services": services,
        "uptime": uptime,
        "version": "2.0.0",
        "architecture": "independent_producer_consumer",
        "kafka_integration": "enabled"
    }


# Helper Functions
def get_kafka_topic_for_priority(priority: str) -> str:
    """Get appropriate Kafka topic based on priority"""
    topic_map = {
        "urgent": "video-processing-urgent",
        "high": "video-processing-high", 
        "normal": "video-processing-normal"
    }
    return topic_map.get(priority, "video-processing-normal")

def estimate_completion_time(priority: str) -> str:
    """Estimate completion time based on priority"""
    base_times = {
        "urgent": 30,
        "high": 60,
        "normal": 120
    }
    
    base_time = base_times.get(priority, 120)
    estimated_time = datetime.fromtimestamp(time.time() + base_time)
    return estimated_time.isoformat()

async def store_job_info(job_id: str, job_data: Dict):
    """Store initial job information in Redis"""
    if redis_client:
        try:
            redis_client.setex(
                f"job:{job_id}",
                3600,  # 1 hour TTL
                json.dumps(job_data)
            )
            print(f"✅ [Producer] Stored job {job_id} in Redis")
        except Exception as e:
            print(f"❌ [Producer] Failed to store job info: {e}")

async def get_job_info(job_id: str) -> Optional[Dict]:
    """Get job information from Redis"""
    if redis_client:
        try:
            job_data = redis_client.get(f"job:{job_id}")
            return json.loads(job_data) if job_data else None
        except Exception as e:
            print(f"❌ [Producer] Failed to get job info: {e}")
            return None
    return None

def extract_video_id(youtube_url: str) -> str:
    """Extract video ID from YouTube URL"""
    import re
    
    patterns = [
        r'(?:v=|\/)([0-9A-Za-z_-]{11}).*',
        r'(?:embed\/)([0-9A-Za-z_-]{11})',
        r'(?:vi\/)([0-9A-Za-z_-]{11})',
        r'(?:youtu\.be\/)([0-9A-Za-z_-]{11})'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, youtube_url)
        if match:
            return match.group(1)
    
    raise ValueError(f"Could not extract video ID from URL: {youtube_url}")

# Run the application
if __name__ == "__main__":
    print("🚀 BABBL LABS FASTAPI PRODUCER SERVICE")
    print("="*50) 
    print("📋 Independent FastAPI producer service")
    print("🔗 Sends jobs to: Kafka topics")
    print("📊 Reads status from: Redis (updated by consumer)")
    print("⚡ Consumer runs separately in another terminal")
    print("="*50)
    print("📚 API Documentation: http://localhost:8080/docs")
    print("🏥 Health Check: http://localhost:8080/health")
    print("🎬 Landing Page: http://localhost:8080")
    print("="*50)
    
    uvicorn.run(
        "fastapi_producer:app",
        host="0.0.0.0",
        port=8080,
        reload=False,
        log_level="info"
    )