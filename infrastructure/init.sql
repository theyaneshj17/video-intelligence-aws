-- Babbl Labs Data Pipeline Schema - FIXED VERSION
-- This version handles all constraint issues properly

-- Video metadata table
CREATE TABLE IF NOT EXISTS videos (
    video_id VARCHAR(20) PRIMARY KEY,
    video_url TEXT NOT NULL,
    title TEXT,
    duration INTEGER,
    channel VARCHAR(255),
    processed_at TIMESTAMP DEFAULT NOW(),
    processing_time FLOAT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Analysis results table with UNIQUE constraint built-in
CREATE TABLE IF NOT EXISTS video_analysis (
    id SERIAL PRIMARY KEY,
    video_id VARCHAR(20) UNIQUE REFERENCES videos(video_id), -- UNIQUE constraint added here
    business_insights TEXT,
    entity_analysis TEXT,
    sentiment_summary TEXT,
    competitive_intelligence TEXT,
    financial_implications TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Batch processing tracking
CREATE TABLE IF NOT EXISTS batch_processing (
    batch_id VARCHAR(50) PRIMARY KEY,
    video_count INTEGER,
    successful_count INTEGER,
    failed_count INTEGER,
    processing_time FLOAT,
    throughput FLOAT,
    sla_compliance_rate FLOAT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Error tracking
CREATE TABLE IF NOT EXISTS processing_errors (
    id SERIAL PRIMARY KEY,
    video_id VARCHAR(20),
    video_url TEXT,
    error_message TEXT,
    error_timestamp TIMESTAMP DEFAULT NOW(),
    processing_time FLOAT
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_videos_processed_at ON videos(processed_at);
CREATE INDEX IF NOT EXISTS idx_video_analysis_video_id ON video_analysis(video_id);
CREATE INDEX IF NOT EXISTS idx_batch_processing_created_at ON batch_processing(created_at);
CREATE INDEX IF NOT EXISTS idx_errors_timestamp ON processing_errors(error_timestamp);

-- Insert test data
INSERT INTO videos (video_id, video_url, title, channel) 
VALUES ('test123', 'https://youtube.com/watch?v=test123', 'Test Video', 'Test Channel')
ON CONFLICT (video_id) DO NOTHING;

-- Verify the schema
SELECT 'Schema created successfully!' as status;