# transcript_extractor.py
# Handles YouTube transcript extraction with separation of concerns

import re
import json
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime

from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, VideoUnavailable


@dataclass
class TranscriptSegment:
    """Data class for transcript segments"""
    start: float
    duration: float
    text: str
    end: float = None
    
    def __post_init__(self):
        if self.end is None:
            self.end = self.start + self.duration


@dataclass
class VideoMetadata:
    """Data class for video metadata"""
    video_id: str
    url: str
    title: Optional[str] = None
    channel: Optional[str] = None
    duration: Optional[int] = None
    view_count: Optional[int] = None


class YouTubeTranscriptExtractor:
    """
    Handles YouTube transcript extraction with error handling and caching
    Separation of concerns: Only responsible for transcript retrieval
    """
    
    def __init__(self, cache_enabled: bool = True):
        """
        Initialize transcript extractor
        
        :param cache_enabled: Whether to enable transcript caching
        """
        self.cache_enabled = cache_enabled
        self._transcript_cache = {}
        
    def extract_video_id(self, youtube_url: str) -> str:
        """
        Extract video ID from various YouTube URL formats
        
        :param youtube_url: YouTube URL in any format
        :return: Video ID string
        """
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
    
    def get_available_transcripts(self, video_id: str) -> List[Dict]:
        """
        Get list of available transcript languages for a video
        
        :param video_id: YouTube video ID
        :return: List of available transcript information
        """
        try:
            transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
            
            available = []
            for transcript in transcript_list:
                available.append({
                    'language': transcript.language,
                    'language_code': transcript.language_code,
                    'is_generated': transcript.is_generated,
                    'is_translatable': transcript.is_translatable
                })
            
            return available
            
        except Exception as e:
            print(f"⚠️  Could not list transcripts for {video_id}: {e}")
            return []
    
    def fetch_transcript(self, video_id: str, languages: List[str] = None) -> List[TranscriptSegment]:
        """
        Fetch transcript for a video with intelligent language fallback
        
        :param video_id: YouTube video ID
        :param languages: Preferred languages (e.g., ['en', 'en-US'])
        :return: List of transcript segments
        """
        if languages is None:
            languages = ['en', 'en-US', 'en-GB']
        
        # Check cache first
        cache_key = f"{video_id}_{'-'.join(languages)}"
        if self.cache_enabled and cache_key in self._transcript_cache:
            print(f"📦 Using cached transcript for {video_id}")
            return self._transcript_cache[cache_key]
        
        try:
            print(f"📥 Fetching transcript for video: {video_id}")
            
            # Use the new API format
            api = YouTubeTranscriptApi()
            
            # Try preferred languages first
            fetched_transcript = None
            for lang in languages:
                try:
                    fetched_transcript = api.fetch(video_id, languages=[lang])
                    print(f"✅ Got transcript in language: {lang}")
                    break
                except:
                    continue
            
            # If preferred languages fail, try any available transcript
            if fetched_transcript is None:
                try:
                    fetched_transcript = api.fetch(video_id)
                    print("✅ Got transcript in default language")
                except Exception as e:
                    raise Exception(f"No transcript available: {str(e)}")
            
            # Convert the new format to our data structure
            segments = []
            for snippet in fetched_transcript.snippets:
                segment = TranscriptSegment(
                    start=snippet.start,
                    duration=snippet.duration,
                    text=snippet.text
                )
                segments.append(segment)
            
            # Cache the result
            if self.cache_enabled:
                self._transcript_cache[cache_key] = segments
            
            print(f"✅ Extracted {len(segments)} transcript segments")
            return segments
            
        except TranscriptsDisabled:
            raise Exception("Transcripts are disabled for this video")
        except VideoUnavailable:
            raise Exception("Video is unavailable or private")
        except Exception as e:
            raise Exception(f"Failed to fetch transcript: {str(e)}")
    
    def extract_from_url(self, youtube_url: str, languages: List[str] = None) -> Dict:
        """
        Complete extraction pipeline from URL to structured transcript data
        
        :param youtube_url: YouTube video URL
        :param languages: Preferred languages
        :return: Complete transcript data structure
        """
        # Extract video ID
        video_id = self.extract_video_id(youtube_url)
        
        # Get available transcripts info
        available_transcripts = self.get_available_transcripts(video_id)
        
        # Fetch transcript segments
        segments = self.fetch_transcript(video_id, languages)
        
        # Create full text
        full_text = " ".join([segment.text for segment in segments])
        
        # Create metadata
        metadata = VideoMetadata(
            video_id=video_id,
            url=youtube_url
        )
        
        return {
            'metadata': metadata,
            'segments': segments,
            'full_text': full_text,
            'available_transcripts': available_transcripts,
            'extraction_timestamp': datetime.now().isoformat(),
            'segment_count': len(segments),
            'character_count': len(full_text)
        }
    
    def clear_cache(self):
        """Clear transcript cache"""
        self._transcript_cache.clear()
        print("🗑️  Transcript cache cleared")


# Example usage and testing
if __name__ == "__main__":
    # Test the extractor
    extractor = YouTubeTranscriptExtractor()
    
    # Test with your video
    video_url = "https://www.youtube.com/watch?v=0X0Jm8QValY"
    
    try:
        print("🧪 Testing YouTube Transcript Extractor")
        print("=" * 50)
        
        # Extract transcript
        result = extractor.extract_from_url(video_url)
        
        print(f"📹 Video ID: {result['metadata'].video_id}")
        print(f"📊 Segments: {result['segment_count']}")
        print(f"📝 Characters: {result['character_count']}")
        
        print(f"\n📋 Available transcripts:")
        for transcript in result['available_transcripts']:
            status = "auto" if transcript['is_generated'] else "manual"
            print(f"   • {transcript['language']} ({status})")
        
        print(f"\n📄 First 3 segments:")
        for i, segment in enumerate(result['segments'][:3]):
            print(f"   {i+1}. [{segment.start:.2f}s] {segment.text}")
        
        print(f"\n🎯 Text preview: {result['full_text'][:200]}...")
        
        # Save to file for other modules to use
        with open('extracted_transcript.json', 'w') as f:
            # Convert segments to dict for JSON serialization
            json_data = {
                'metadata': {
                    'video_id': result['metadata'].video_id,
                    'url': result['metadata'].url
                },
                'segments': [
                    {
                        'start': seg.start,
                        'duration': seg.duration,
                        'text': seg.text,
                        'end': seg.end
                    } for seg in result['segments']
                ],
                'full_text': result['full_text'],
                'extraction_timestamp': result['extraction_timestamp'],
                'segment_count': result['segment_count'],
                'character_count': result['character_count']
            }
            json.dump(json_data, f, indent=2)
        
        print(f"\n💾 Transcript saved to extracted_transcript.json")
        
    except Exception as e:
        print(f"❌ Error: {e}")