#!/usr/bin/env python3
"""
ChromaDB Vector Storage Integration
Handles video transcript embeddings and semantic search
"""

import os
import sys
import json
import time
import numpy as np
from typing import List, Dict, Any
from datetime import datetime

# Add project path
current_dir = os.getcwd()
sys.path.append(current_dir)
sys.path.append(os.path.join(current_dir, 'services'))

try:
    import chromadb
    from chromadb.config import Settings
    from sentence_transformers import SentenceTransformer
except ImportError as e:
    print(f"❌ Missing dependency: {e}")
    print("Install with: pip install chromadb sentence-transformers")
    sys.exit(1)

class BabblVectorStore:
    """
    ChromaDB integration for video transcript vector storage and retrieval
    Enables semantic search across video content for institutional clients
    """
    
    def __init__(self, host: str = "localhost", port: int = 8001):
        print("🧠 Initializing Babbl Vector Store...")
        
        # Initialize ChromaDB client
        try:
            self.client = chromadb.HttpClient(
                host=host,
                port=port,
                settings=Settings(allow_reset=True)
            )
            
            # Test connection
            self.client.heartbeat()
            print(f"✅ ChromaDB connected at {host}:{port}")
            
        except Exception as e:
            print(f"❌ ChromaDB connection failed: {e}")
            print("   Make sure ChromaDB is running: docker-compose up -d chromadb")
            sys.exit(1)
        
        # Initialize embedding model
        try:
            self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
            print("✅ Sentence transformer model loaded")
        except Exception as e:
            print(f"❌ Failed to load embedding model: {e}")
            sys.exit(1)
        
        # Initialize collections
        self.collections = {
            'video_transcripts': self._get_or_create_collection(
                'video_transcripts',
                metadata={"description": "Video transcript chunks for semantic search"}
            ),
            'video_summaries': self._get_or_create_collection(
                'video_summaries', 
                metadata={"description": "Video analysis summaries"}
            ),
            'entities': self._get_or_create_collection(
                'entities',
                metadata={"description": "Extracted entities and sentiment"}
            )
        }
        
        print("✅ Vector store ready!")
    
    def _get_or_create_collection(self, name: str, metadata: Dict = None):
        """Get existing collection or create new one"""
        try:
            collection = self.client.get_collection(name)
            print(f"📚 Found existing collection: {name}")
            return collection
        except:
            collection = self.client.create_collection(
                name=name,
                metadata=metadata or {}
            )
            print(f"📚 Created new collection: {name}")
            return collection
    
    def create_transcript_embeddings(self, transcript_data: Dict) -> List[Dict]:
        """
        Create embeddings for transcript segments
        Splits transcript into semantic chunks for better search
        """
        segments = transcript_data.get('segments', [])
        if not segments:
            return []
        
        print(f"🔢 Creating embeddings for {len(segments)} transcript segments...")
        
        # Group segments into meaningful chunks (30-60 seconds each)
        chunks = self._create_semantic_chunks(segments)
        
        # Generate embeddings for each chunk
        chunk_texts = [chunk['text'] for chunk in chunks]
        embeddings = self.embedding_model.encode(chunk_texts).tolist()
        
        # Combine chunks with embeddings
        for i, chunk in enumerate(chunks):
            chunk['embedding'] = embeddings[i]
            chunk['embedding_model'] = 'all-MiniLM-L6-v2'
            chunk['created_at'] = datetime.now().isoformat()
        
        print(f"✅ Created {len(chunks)} semantic chunks with embeddings")
        return chunks
    
    def _create_semantic_chunks(self, segments: List) -> List[Dict]:
        """Create semantic chunks from transcript segments"""
        chunks = []
        current_chunk = {
            'text': '',
            'start_time': 0,
            'end_time': 0,
            'segment_count': 0
        }
        
        chunk_duration = 45  # Target 45 seconds per chunk
        max_chunk_length = 500  # Max 500 characters per chunk
        
        for segment in segments:
            # Extract segment data (handle both dict and object formats)
            if hasattr(segment, 'text'):
                text = segment.text
                start = segment.start
                end = segment.end if hasattr(segment, 'end') else segment.start + segment.duration
            else:
                text = segment.get('text', '')
                start = segment.get('start', 0)
                end = segment.get('end', start + segment.get('duration', 0))
            
            # Start new chunk if needed
            if current_chunk['segment_count'] == 0:
                current_chunk['start_time'] = start
            
            # Add segment to current chunk
            current_chunk['text'] += ' ' + text
            current_chunk['end_time'] = end
            current_chunk['segment_count'] += 1
            
            # Check if chunk should be finalized
            chunk_length = end - current_chunk['start_time']
            text_length = len(current_chunk['text'])
            
            if (chunk_length >= chunk_duration or 
                text_length >= max_chunk_length or
                segment == segments[-1]):  # Last segment
                
                # Finalize current chunk
                if current_chunk['text'].strip():
                    chunks.append({
                        'text': current_chunk['text'].strip(),
                        'start_time': current_chunk['start_time'],
                        'end_time': current_chunk['end_time'],
                        'duration': current_chunk['end_time'] - current_chunk['start_time'],
                        'segment_count': current_chunk['segment_count']
                    })
                
                # Reset for next chunk
                current_chunk = {
                    'text': '',
                    'start_time': 0,
                    'end_time': 0,
                    'segment_count': 0
                }
        
        return chunks
    
    def store_video_embeddings(self, video_id: str, video_data: Dict):
        """Store all video embeddings in ChromaDB"""
        print(f"💾 Storing embeddings for video {video_id}...")
        
        try:
            # 1. Store transcript chunks
            transcript_data = video_data.get('transcript_data', {})
            if transcript_data.get('segments'):
                chunks = self.create_transcript_embeddings(transcript_data)
                self._store_transcript_chunks(video_id, chunks, video_data)
            
            # 2. Store analysis summary
            analysis_results = video_data.get('analysis_results', {})
            if analysis_results:
                self._store_analysis_summary(video_id, analysis_results, video_data)
            
            # 3. Store entities
            entity_analysis = analysis_results.get('entity_analysis', '')
            if entity_analysis:
                self._store_entities(video_id, entity_analysis, video_data)
            
            print(f"✅ Vector storage complete for {video_id}")
            
        except Exception as e:
            print(f"❌ Vector storage error for {video_id}: {e}")
    
    def _store_transcript_chunks(self, video_id: str, chunks: List[Dict], video_data: Dict):
        """Store transcript chunks in ChromaDB"""
        if not chunks:
            return
        
        # Prepare data for ChromaDB
        ids = [f"{video_id}_chunk_{i}" for i in range(len(chunks))]
        documents = [chunk['text'] for chunk in chunks]
        embeddings = [chunk['embedding'] for chunk in chunks]
        metadatas = []
        
        for i, chunk in enumerate(chunks):
            metadata = {
                'video_id': video_id,
                'video_url': video_data.get('video_url', ''),
                'chunk_index': i,
                'start_time': chunk['start_time'],
                'end_time': chunk['end_time'],
                'duration': chunk['duration'],
                'segment_count': chunk['segment_count'],
                'created_at': chunk['created_at'],
                'content_type': 'transcript_chunk'
            }
            metadatas.append(metadata)
        
        # Store in ChromaDB
        self.collections['video_transcripts'].add(
            ids=ids,
            documents=documents,
            embeddings=embeddings,
            metadatas=metadatas
        )
        
        print(f"   📝 Stored {len(chunks)} transcript chunks")
    
    def _store_analysis_summary(self, video_id: str, analysis_results: Dict, video_data: Dict):
        """Store analysis summary as searchable vector"""
        
        # Combine all analysis into searchable text
        summary_text = f"""
        Business Insights: {analysis_results.get('business_insights', '')}
        
        Competitive Intelligence: {analysis_results.get('competitive_intelligence', '')}
        
        Financial Implications: {analysis_results.get('financial_implications', '')}
        
        Sentiment Summary: {analysis_results.get('sentiment_summary', '')}
        """
        
        # Generate embedding
        embedding = self.embedding_model.encode([summary_text.strip()])[0].tolist()
        
        # Store in ChromaDB
        self.collections['video_summaries'].add(
            ids=[f"{video_id}_summary"],
            documents=[summary_text.strip()],
            embeddings=[embedding],
            metadatas=[{
                'video_id': video_id,
                'video_url': video_data.get('video_url', ''),
                'created_at': datetime.now().isoformat(),
                'content_type': 'analysis_summary',
                'processing_time': video_data.get('processing_metrics', {}).get('total_time', 0)
            }]
        )
        
        print(f"   📊 Stored analysis summary")
    
    def _store_entities(self, video_id: str, entity_analysis: str, video_data: Dict):
        """Store extracted entities for entity-based search"""
        
        # Parse entities from analysis text (simplified parsing)
        entities = self._parse_entities(entity_analysis)
        
        if not entities:
            return
        
        # Generate embeddings for entity descriptions
        entity_texts = [f"{entity['name']}: {entity['description']}" for entity in entities]
        embeddings = self.embedding_model.encode(entity_texts).tolist()
        
        # Prepare data for ChromaDB
        ids = [f"{video_id}_entity_{i}" for i in range(len(entities))]
        metadatas = []
        
        for i, entity in enumerate(entities):
            metadata = {
                'video_id': video_id,
                'video_url': video_data.get('video_url', ''),
                'entity_name': entity['name'],
                'entity_type': entity['type'],
                'sentiment_score': entity['sentiment'],
                'created_at': datetime.now().isoformat(),
                'content_type': 'entity'
            }
            metadatas.append(metadata)
        
        # Store in ChromaDB
        self.collections['entities'].add(
            ids=ids,
            documents=entity_texts,
            embeddings=embeddings,
            metadatas=metadatas
        )
        
        print(f"   🏢 Stored {len(entities)} entities")
    
    def _parse_entities(self, entity_analysis: str) -> List[Dict]:
        """Parse entities from analysis text"""
        entities = []
        
        try:
            lines = entity_analysis.split('\n')
            current_section = None
            
            for line in lines:
                line = line.strip()
                
                # Detect section headers
                if line.upper() in ['COMPANIES:', 'PRODUCTS:', 'PEOPLE:', 'KEY TOPICS:']:
                    current_section = line.replace(':', '').lower()
                    continue
                
                # Parse entity lines (format: "- Name: sentiment_score - description")
                if line.startswith('-') and ':' in line and current_section:
                    try:
                        # Split on first colon
                        name_part, rest = line[1:].split(':', 1)
                        name = name_part.strip()
                        
                        # Extract sentiment score if present
                        sentiment = 0.0
                        description = rest.strip()
                        
                        # Look for sentiment score pattern
                        import re
                        score_match = re.search(r'([+-]?\d*\.?\d+)', rest)
                        if score_match:
                            sentiment = float(score_match.group(1))
                            # Remove score from description
                            description = re.sub(r'[+-]?\d*\.?\d+\s*-?\s*', '', rest).strip()
                        
                        entities.append({
                            'name': name,
                            'type': current_section.rstrip('s'),  # Remove plural
                            'sentiment': sentiment,
                            'description': description
                        })
                    except:
                        continue
        
        except Exception as e:
            print(f"⚠️ Entity parsing error: {e}")
        
        return entities
    
    def semantic_search(self, query: str, collection_name: str = 'video_transcripts',
                       n_results: int = 10, filters: Dict = None) -> Dict:
        """
        Perform semantic search across video content
        Used by institutional clients for finding relevant content
        """
        print(f"🔍 Searching for: '{query}' in {collection_name}")
        
        try:
            # Generate query embedding
            query_embedding = self.embedding_model.encode([query])[0].tolist()
            
            # Prepare search parameters
            search_params = {
                'query_embeddings': [query_embedding],
                'n_results': n_results
            }
            
            # Add filters if provided
            if filters:
                search_params['where'] = filters
            
            # Perform search
            results = self.collections[collection_name].query(**search_params)
            
            # Format results
            formatted_results = {
                'query': query,
                'collection': collection_name,
                'total_results': len(results['ids'][0]),
                'results': []
            }
            
            for i in range(len(results['ids'][0])):
                result = {
                    'id': results['ids'][0][i],
                    'document': results['documents'][0][i],
                    'metadata': results['metadatas'][0][i],
                    'distance': results['distances'][0][i],
                    'similarity_score': 1 - results['distances'][0][i]  # Convert distance to similarity
                }
                formatted_results['results'].append(result)
            
            print(f"✅ Found {len(formatted_results['results'])} results")
            return formatted_results
            
        except Exception as e:
            print(f"❌ Search error: {e}")
            return {'query': query, 'results': [], 'error': str(e)}
    
    def search_by_company(self, company_name: str, sentiment_filter: str = None) -> Dict:
        """Search for videos mentioning specific companies"""
        
        # Simple ChromaDB doesn't support complex filters like MongoDB
        # We'll do post-filtering instead
        
        results = self.semantic_search(
            query=f"{company_name} company business",
            collection_name='entities',
            n_results=20  # Get more results to filter
        )
        
        # Post-filter by sentiment if requested
        if sentiment_filter and results.get('results'):
            filtered_results = []
            for result in results['results']:
                metadata = result.get('metadata', {})
                sentiment_score = metadata.get('sentiment_score', 0)
                
                if sentiment_filter.lower() == 'positive' and sentiment_score > 0.2:
                    filtered_results.append(result)
                elif sentiment_filter.lower() == 'negative' and sentiment_score < -0.2:
                    filtered_results.append(result)
                elif sentiment_filter.lower() == 'neutral':
                    filtered_results.append(result)
            
            results['results'] = filtered_results
            results['total_results'] = len(filtered_results)
        
        return results
    
    def search_financial_implications(self, query: str) -> Dict:
        """Search for financial implications across videos"""
        
        financial_query = f"financial implications revenue earnings profit loss investment {query}"
        
        return self.semantic_search(
            query=financial_query,
            collection_name='video_summaries'
        )
    
    def search_competitive_intelligence(self, company1: str, company2: str = None) -> Dict:
        """Search for competitive intelligence mentions"""
        
        if company2:
            query = f"{company1} vs {company2} competition competitive advantage market share"
        else:
            query = f"{company1} competitive intelligence market position strategy"
        
        return self.semantic_search(
            query=query,
            collection_name='video_summaries'
        )
    
    def get_video_timeline_search(self, video_id: str, query: str) -> Dict:
        """Search within specific video timeline"""
        
        filters = {'video_id': video_id}
        
        results = self.semantic_search(
            query=query,
            collection_name='video_transcripts',
            filters=filters
        )
        
        # Sort by timestamp for timeline view
        if results['results']:
            results['results'].sort(key=lambda x: x['metadata'].get('start_time', 0))
        
        return results
    
    def get_collection_stats(self) -> Dict:
        """Get statistics about stored vectors"""
        stats = {}
        
        for name, collection in self.collections.items():
            try:
                count = collection.count()
                stats[name] = {
                    'document_count': count,
                    'status': 'healthy'
                }
            except Exception as e:
                stats[name] = {
                    'document_count': 0,
                    'status': f'error: {e}'
                }
        
        return stats
    
    def delete_video_embeddings(self, video_id: str):
        """Delete all embeddings for a specific video"""
        print(f"🗑️ Deleting embeddings for video {video_id}")
        
        for name, collection in self.collections.items():
            try:
                # Get all IDs for this video
                results = collection.get(where={'video_id': video_id})
                
                if results['ids']:
                    collection.delete(ids=results['ids'])
                    print(f"   Deleted {len(results['ids'])} items from {name}")
            
            except Exception as e:
                print(f"   ⚠️ Error deleting from {name}: {e}")

def test_vector_store():
    """Test the vector store functionality"""
    print("🧪 Testing Vector Store")
    print("="*50)
    
    # Initialize vector store
    vector_store = BabblVectorStore()
    
    # Create test video data
    test_video_data = {
        'video_url': 'https://youtube.com/watch?v=test123',
        'transcript_data': {
            'segments': [
                {'text': 'Apple released the new iPhone with improved features', 'start': 0, 'duration': 3},
                {'text': 'The competitive landscape is changing rapidly', 'start': 3, 'duration': 3},
                {'text': 'Tesla is leading in electric vehicle innovation', 'start': 6, 'duration': 3},
                {'text': 'Financial markets are responding positively', 'start': 9, 'duration': 3}
            ]
        },
        'analysis_results': {
            'business_insights': 'Apple shows strong product innovation with new iPhone release',
            'entity_analysis': 'COMPANIES:\n- Apple: +0.8 - Positive product launch\n- Tesla: +0.6 - Innovation leader',
            'competitive_intelligence': 'Apple maintains competitive edge through innovation',
            'financial_implications': 'Positive market response expected for Apple stock'
        },
        'processing_metrics': {'total_time': 25.5}
    }
    
    # Store embeddings
    vector_store.store_video_embeddings('test123', test_video_data)
    
    # Test searches
    print(f"\n🔍 Testing semantic searches...")
    
    # Test transcript search
    transcript_results = vector_store.semantic_search("Apple iPhone innovation")
    print(f"📱 iPhone search results: {len(transcript_results['results'])}")
    
    # Test company search
    company_results = vector_store.search_by_company("Apple", "positive")
    print(f"🏢 Apple company search: {len(company_results['results'])}")
    
    # Test financial search
    financial_results = vector_store.search_financial_implications("stock market")
    print(f"💰 Financial search results: {len(financial_results['results'])}")
    
    # Show stats
    stats = vector_store.get_collection_stats()
    print(f"\n📊 Collection Statistics:")
    for collection, stat in stats.items():
        print(f"   {collection}: {stat['document_count']} documents ({stat['status']})")
    
    print(f"\n✅ Vector store test complete!")

if __name__ == "__main__":
    test_vector_store()