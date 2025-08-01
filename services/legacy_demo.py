# main_demo.py
# Complete video intelligence demo orchestrating all components
# Separation of concerns: Main orchestration and user interface

import json
import time
from datetime import datetime
from typing import Dict, Any
import argparse

# Import our modules
from transcript_extractor import YouTubeTranscriptExtractor
from langchain_analyzer import BabblIntelligenceAnalyzer, AnalysisResult


class BabblVideoIntelligenceDemo:
    """
    Complete video intelligence demonstration system
    Orchestrates transcript extraction and AI analysis
    """
    
    def __init__(self):
        """Initialize the complete demo system"""
        print("🎬 Initializing Babbl Video Intelligence Demo")
        print("=" * 60)
        
        # Initialize components
        self.transcript_extractor = YouTubeTranscriptExtractor(cache_enabled=True)
        self.intelligence_analyzer = BabblIntelligenceAnalyzer()
        
        print("✅ All systems ready!")
    
    def process_video_url(self, youtube_url: str, save_results: bool = True) -> Dict[str, Any]:
        """
        Complete video processing pipeline from URL to insights
        
        :param youtube_url: YouTube video URL
        :param save_results: Whether to save results to file
        :return: Complete analysis results
        """
        overall_start_time = time.time()
        
        print(f"\n🎥 PROCESSING VIDEO: {youtube_url}")
        print("=" * 80)
        
        try:
            # Step 1: Extract transcript
            print("📥 STEP 1: Extracting transcript...")
            extract_start = time.time()
            
            transcript_data = self.transcript_extractor.extract_from_url(youtube_url)
            
            extract_time = time.time() - extract_start
            print(f"✅ Transcript extracted in {extract_time:.2f} seconds")
            print(f"   📊 {transcript_data['segment_count']} segments, {transcript_data['character_count']} characters")
            
            # Step 2: Run AI analysis
            print(f"\n🧠 STEP 2: Running AI intelligence analysis...")
            analysis_start = time.time()
            
            analysis_results = self.intelligence_analyzer.comprehensive_analysis(
                transcript_data['full_text']
            )
            
            analysis_time = time.time() - analysis_start
            print(f"✅ Intelligence analysis completed in {analysis_time:.2f} seconds")
            
            # Step 3: Compile complete results
            total_time = time.time() - overall_start_time
            
            complete_results = {
                'video_metadata': {
                    'video_id': transcript_data['metadata'].video_id,
                    'url': transcript_data['metadata'].url,
                    'processed_at': datetime.now().isoformat(),
                    'total_processing_time': round(total_time, 2)
                },
                'transcript_info': {
                    'segment_count': transcript_data['segment_count'],
                    'character_count': transcript_data['character_count'],
                    'extraction_time': round(extract_time, 2),
                    'available_languages': len(transcript_data['available_transcripts'])
                },
                'intelligence_analysis': {
                    'business_insights': analysis_results.business_insights,
                    'entity_analysis': analysis_results.entity_analysis,
                    'sentiment_summary': analysis_results.sentiment_summary,
                    'competitive_intelligence': analysis_results.competitive_intelligence,
                    'financial_implications': analysis_results.financial_implications,
                    'analysis_time': round(analysis_results.processing_time, 2)
                },
                'performance_metrics': {
                    'transcript_extraction_time': round(extract_time, 2),
                    'ai_analysis_time': round(analysis_results.processing_time, 2),
                    'total_processing_time': round(total_time, 2),
                    'characters_per_second': round(transcript_data['character_count'] / total_time, 2)
                }
            }
            
            # Save results if requested
            if save_results:
                self._save_results(complete_results)
            
            return complete_results
            
        except Exception as e:
            error_result = {
                'error': str(e),
                'video_url': youtube_url,
                'timestamp': datetime.now().isoformat()
            }
            print(f"❌ Error processing video: {e}")
            return error_result
    
    def display_results(self, results: Dict[str, Any]):
        """
        Display results in a formatted, professional manner
        
        :param results: Complete analysis results
        """
        if 'error' in results:
            print(f"❌ Processing failed: {results['error']}")
            return
        
        print("\n" + "="*80)
        print("🎯 BABBL VIDEO INTELLIGENCE RESULTS")
        print("="*80)
        
        # Video information
        video_info = results['video_metadata']
        transcript_info = results['transcript_info']
        
        print(f"📹 Video ID: {video_info['video_id']}")
        print(f"🔗 URL: {video_info['url']}")
        print(f"⏱️  Total Processing Time: {video_info['total_processing_time']} seconds")
        print(f"📊 Transcript: {transcript_info['segment_count']} segments, {transcript_info['character_count']:,} characters")
        
        # Performance metrics
        perf = results['performance_metrics']
        print(f"\n📈 PERFORMANCE METRICS:")
        print(f"   • Transcript Extraction: {perf['transcript_extraction_time']}s")
        print(f"   • AI Analysis: {perf['ai_analysis_time']}s")
        print(f"   • Processing Speed: {perf['characters_per_second']:,.0f} chars/sec")
        
        # Intelligence analysis results
        analysis = results['intelligence_analysis']
        
        print(f"\n💼 BUSINESS INSIGHTS:")
        print("-" * 40)
        print(analysis['business_insights'])
        
        print(f"\n🏢 ENTITY & SENTIMENT ANALYSIS:")
        print("-" * 40)
        print(analysis['entity_analysis'])
        
        print(f"\n💭 SENTIMENT SUMMARY:")
        print("-" * 40)
        print(analysis['sentiment_summary'])
        
        print(f"\n⚔️ COMPETITIVE INTELLIGENCE:")
        print("-" * 40)
        print(analysis['competitive_intelligence'])
        
        print(f"\n💰 FINANCIAL IMPLICATIONS:")
        print("-" * 40)
        print(analysis['financial_implications'])
    
    def interactive_query_session(self, transcript_text: str):
        """
        Interactive session for querying video content
        
        :param transcript_text: Full transcript text for querying
        """
        print(f"\n🔍 INTERACTIVE QUERY SESSION")
        print("="*60)
        print("Ask questions about the video content. Type 'quit' to exit.")
        print("Example: 'What companies are mentioned?', 'What's the sentiment toward Apple?'")
        
        while True:
            try:
                question = input("\n❓ Your question: ").strip()
                
                if question.lower() in ['quit', 'exit', 'q']:
                    print("👋 Ending query session")
                    break
                
                if not question:
                    print("Please enter a question")
                    continue
                
                print(f"🔍 Searching...")
                answer = self.intelligence_analyzer.query_content(transcript_text, question)
                
                print(f"\n💡 Answer:")
                print(f"{answer}")
                
            except KeyboardInterrupt:
                print("\n👋 Query session interrupted")
                break
            except Exception as e:
                print(f"❌ Error answering question: {e}")
    
    def demo_predefined_queries(self, transcript_text: str):
        """
        Run predefined demo queries to showcase capabilities
        
        :param transcript_text: Full transcript text
        """
        demo_questions = [
            "What companies or brands are mentioned in this video?",
            "What is the overall sentiment toward Apple in this video?",
            "Are there any negative opinions expressed about any products?",
            "What competitive comparisons are made?",
            "Are there any mentions of pricing or costs?",
            "What technology trends are discussed?"
        ]
        
        print(f"\n🎯 DEMO QUERIES - Showcasing RAG Capabilities")
        print("="*60)
        
        for i, question in enumerate(demo_questions, 1):
            print(f"\n{i}. ❓ {question}")
            print("-" * 50)
            
            try:
                answer = self.intelligence_analyzer.query_content(transcript_text, question)
                print(f"💡 {answer}")
            except Exception as e:
                print(f"❌ Error: {e}")
            
            # Small delay for demo effect
            time.sleep(1)
    
    def _save_results(self, results: Dict[str, Any]):
        """
        Save analysis results to JSON file
        
        :param results: Complete results to save
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        video_id = results['video_metadata']['video_id']
        filename = f"babbl_analysis_{video_id}_{timestamp}.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump(results, f, indent=2)
            print(f"💾 Results saved to: {filename}")
        except Exception as e:
            print(f"⚠️ Could not save results: {e}")
    
    def run_complete_demo(self, youtube_url: str, interactive: bool = True):
        """
        Run the complete demonstration pipeline
        
        :param youtube_url: YouTube URL to process
        :param interactive: Whether to include interactive query session
        """
        print("🚀 STARTING COMPLETE BABBL VIDEO INTELLIGENCE DEMO")
        print("="*80)
        
        # Process the video
        results = self.process_video_url(youtube_url)
        
        if 'error' in results:
            return results
        
        # Display results
        self.display_results(results)
        
        # Get transcript for querying
        transcript_data = self.transcript_extractor.extract_from_url(youtube_url)
        transcript_text = transcript_data['full_text']
        
        # Run demo queries
        self.demo_predefined_queries(transcript_text)
        
        # Interactive session if requested
        if interactive:
            try:
                response = input("\n🤔 Would you like to ask custom questions? (y/n): ").strip().lower()
                if response in ['y', 'yes']:
                    self.interactive_query_session(transcript_text)
            except:
                pass
        
        return results


def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(description='Babbl Video Intelligence Demo')
    parser.add_argument('url', help='YouTube video URL to analyze')
    parser.add_argument('--no-interactive', action='store_true', help='Skip interactive query session')
    parser.add_argument('--no-save', action='store_true', help='Don\'t save results to file')
    
    args = parser.parse_args()
    
    # Initialize demo system
    demo = BabblVideoIntelligenceDemo()
    
    # Run the demo
    results = demo.run_complete_demo(
        youtube_url=args.url,
        interactive=not args.no_interactive
    )
    
    if 'error' not in results:
        print("\n🎉 DEMO COMPLETED SUCCESSFULLY!")
        print("\n📋 KEY CAPABILITIES DEMONSTRATED:")
        print("✅ YouTube transcript extraction")
        print("✅ AI-powered business intelligence analysis")
        print("✅ Entity recognition and sentiment analysis")
        print("✅ Competitive intelligence extraction")
        print("✅ Financial implications assessment")
        print("✅ RAG-style content querying")
        print("✅ Real-time processing pipeline")
        
        print(f"\n🏢 FOR BABBL LABS INTERVIEW:")
        print("• This demonstrates the core video intelligence pipeline")
        print("• Shows scalable architecture with separation of concerns")
        print("• Proves ability to extract actionable business insights")
        print("• Demonstrates hedge fund-relevant financial analysis")
        print("• Shows real-time processing capabilities")
    else:
        print("\n❌ Demo failed - check video URL and try again")


if __name__ == "__main__":
    # For testing without command line args
    if len(__import__('sys').argv) == 1:
        # Default test video
        demo = BabblVideoIntelligenceDemo()
        test_url = "https://www.youtube.com/watch?v=0X0Jm8QValY"
        
        print("🧪 RUNNING TEST DEMO")
        results = demo.run_complete_demo(test_url, interactive=False)
    else:
        main()