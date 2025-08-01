# Enhanced version of your langchain_analyzer.py
# Optimized for pipeline performance and scale

# Your existing imports plus:
import asyncio
from concurrent.futures import ThreadPoolExecutor


import tiktoken
import json
import os
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import asyncio
from datetime import datetime

# LangChain imports (updated for latest version)
from langchain_core.prompts import PromptTemplate
from langchain.chains.question_answering import load_qa_chain
from langchain.chains.summarize import load_summarize_chain
from langchain_anthropic import ChatAnthropic  # Updated import
from langchain.text_splitter import TokenTextSplitter
from langchain.schema import Document

# Import our transcript extractor
from .transcript_service import TranscriptSegment, VideoMetadata


@dataclass
class AnalysisResult:
    """Data class for analysis results"""
    business_insights: str
    entity_analysis: str
    sentiment_summary: str
    competitive_intelligence: str
    financial_implications: str
    processing_time: float



class BabblIntelligenceAnalyzer:
    """Enhanced for data pipeline integration"""
    
    def __init__(self, model_name: str = "claude-3-haiku-20240307", max_output_length: int = 1000, context_window: int = 100000):
        """
        Initialize the intelligence analyzer
        
        :param model_name: Claude model to use
        :param max_output_length: Max tokens in response outputs  
        :param context_window: Context window of the chosen model
        """
        print("🧠 Initializing Babbl Intelligence Analyzer with Claude Haiku...")
        
        # Check for API key
        api_key = os.getenv('ANTHROPIC_API_KEY')
        if not api_key:
            print("⚠️  ANTHROPIC_API_KEY not found in environment variables")
            print("Please set it with: export ANTHROPIC_API_KEY='your-key-here'")
            # For demo purposes, we'll continue with a mock key
            print("🎭 Continuing with demo mode...")
            os.environ['ANTHROPIC_API_KEY'] = 'demo-key'
        
        # Initialize Claude chat model with error handling
        try:
            self.chat = ChatAnthropic(
                model=model_name,
                temperature=0,
                max_tokens=max_output_length,
                anthropic_api_key=api_key or 'demo-key'
            )
            print("✅ Claude Haiku model initialized")
        except Exception as e:
            print(f"⚠️  Could not initialize Claude: {e}")
            print("🎭 Using mock analysis for demo...")
            self.chat = None
        
        # Business Intelligence Analysis Template
        self.business_analysis_template = PromptTemplate(
            template="""
            You are a financial analyst specializing in alternative data and market intelligence.
            Your job is to extract actionable business insights from video content for hedge funds and institutional investors.
            
            VIDEO TRANSCRIPT: {text}
            ANALYSIS REQUEST: {prompt}
            
            Focus on:
            1. Company mentions and sentiment toward each
            2. Product launches, updates, or changes  
            3. Market trends and competitive intelligence
            4. Financial implications and trading opportunities
            5. Risk factors and potential market moving events
            
            Provide specific, actionable intelligence that traders and analysts can use.
            
            FINANCIAL INTELLIGENCE RESPONSE:""",
            input_variables=["prompt", "text"],
        )
        
        # Entity and Sentiment Analysis Template
        self.entity_sentiment_template = PromptTemplate(
            template="""
            Analyze this video transcript to identify and evaluate business entities and sentiment.
            
            TRANSCRIPT: {text}
            
            Extract:
            1. COMPANIES mentioned (with sentiment score -1.0 to +1.0)
            2. PRODUCTS mentioned (with sentiment score -1.0 to +1.0)  
            3. PEOPLE mentioned (with sentiment score -1.0 to +1.0)
            4. KEY TOPICS discussed (with sentiment score -1.0 to +1.0)
            
            Format as structured data:
            
            COMPANIES:
            - [Company Name]: [Sentiment Score] - [Brief explanation]
            
            PRODUCTS:  
            - [Product Name]: [Sentiment Score] - [Brief explanation]
            
            PEOPLE:
            - [Person Name]: [Sentiment Score] - [Brief explanation]
            
            KEY TOPICS:
            - [Topic]: [Sentiment Score] - [Brief explanation]
            
            ENTITY ANALYSIS:""",
            input_variables=["text"],
        )
        
        # Competitive Intelligence Template
        self.competitive_template = PromptTemplate(
            template="""
            Analyze this video transcript for competitive intelligence insights.
            
            TRANSCRIPT: {text}
            
            Identify:
            1. COMPETITIVE COMPARISONS between companies/products
            2. MARKET POSITIONING statements
            3. STRATEGIC ADVANTAGES mentioned
            4. WEAKNESSES or RISKS highlighted
            5. MARKET SHARE or PERFORMANCE discussions
            
            Focus on intelligence that would be valuable for:
            - Investment decisions
            - Competitive analysis
            - Market positioning
            - Risk assessment
            
            COMPETITIVE INTELLIGENCE:""",
            input_variables=["text"],
        )
        
        # Financial Implications Template
        self.financial_template = PromptTemplate(
            template="""
            Analyze this video transcript for financial implications and investment opportunities.
            
            TRANSCRIPT: {text}
            
            Assess:
            1. REVENUE IMPACT discussions
            2. COST STRUCTURE mentions
            3. GROWTH PROSPECTS indicated
            4. RISK FACTORS identified
            5. MARKET OPPORTUNITIES described
            6. VALUATION insights
            
            Provide specific financial intelligence for:
            - Stock price impact potential
            - Earnings implications  
            - Market timing opportunities
            - Risk management insights
            
            FINANCIAL IMPLICATIONS:""",
            input_variables=["text"],
        )
        
        # Initialize chains with error handling
        try:
            self.business_chain = load_summarize_chain(
                self.chat, 
                chain_type="map_reduce", 
                combine_prompt=self.business_analysis_template
            )
            
            self.qa_chain = load_qa_chain(self.chat, chain_type="map_reduce")
            print("✅ LangChain analysis chains initialized")
        except Exception as e:
            print(f"⚠️  Could not initialize chains: {e}")
            self.business_chain = None
            self.qa_chain = None
        
        # Text splitter for long transcripts
        prompt_length = 500  # Approximate prompt length
        self.splitter = TokenTextSplitter(
            chunk_size=context_window - prompt_length - max_output_length,
            chunk_overlap=200,  # Some overlap for context
            model_name="gpt-3.5-turbo"  # For tokenization
        )
        
        print("✅ Intelligence Analyzer ready!")
    
    def _create_documents(self, text: str) -> List[Document]:
        """
        Create LangChain documents from text with intelligent chunking
        
        :param text: Full transcript text
        :return: List of Document objects
        """
        docs = self.splitter.split_documents(
            self.splitter.create_documents([text])
        )
        return docs
    
    def analyze_business_intelligence(self, transcript_text: str, custom_prompt: str = None) -> str:
        """
        Extract comprehensive business intelligence from transcript
        
        :param transcript_text: Full video transcript
        :param custom_prompt: Custom analysis prompt
        :return: Business intelligence analysis
        """
        print("📊 Analyzing business intelligence...")
        
        # If Claude is not available, return mock analysis
        if not self.chat or not self.business_chain:
            return self._mock_business_analysis(transcript_text)
        
        docs = self._create_documents(transcript_text)
        
        if custom_prompt is None:
            custom_prompt = """
            Provide a comprehensive business intelligence analysis focusing on:
            1. Market moving information
            2. Competitive landscape insights  
            3. Investment opportunities and risks
            4. Key financial implications
            5. Strategic business developments
            """
        
        try:
            if len(docs) > 1:
                # Use map-reduce for long transcripts
                result = self.business_chain.run(input_documents=docs, prompt=custom_prompt)
            else:
                # Direct analysis for shorter transcripts
                prompt = self.business_analysis_template.format_prompt(
                    text=transcript_text, 
                    prompt=custom_prompt
                )
                result = self.chat(prompt.to_messages()).content
            
            return result
        except Exception as e:
            print(f"⚠️  Claude analysis failed: {e}, using mock analysis")
            return self._mock_business_analysis(transcript_text)
    
    def analyze_entities_sentiment(self, transcript_text: str) -> str:
        """
        Extract entities and sentiment analysis
        
        :param transcript_text: Full video transcript
        :return: Entity and sentiment analysis
        """
        print("🏢 Analyzing entities and sentiment...")
        
        if not self.chat or not self.qa_chain:
            return self._mock_entity_analysis(transcript_text)
        
        docs = self._create_documents(transcript_text)
        
        try:
            if len(docs) > 1:
                # Use QA chain for entity extraction across long documents
                query = "Extract all companies, products, people, and topics mentioned with their sentiment scores"
                result = self.qa_chain.run(input_documents=docs, question=query)
            else:
                # Direct analysis
                prompt = self.entity_sentiment_template.format_prompt(text=transcript_text)
                result = self.chat(prompt.to_messages()).content
            
            return result
        except Exception as e:
            print(f"⚠️  Entity analysis failed: {e}, using mock analysis")
            return self._mock_entity_analysis(transcript_text)
    
    def analyze_competitive_intelligence(self, transcript_text: str) -> str:
        """
        Extract competitive intelligence insights
        
        :param transcript_text: Full video transcript
        :return: Competitive intelligence analysis
        """
        print("⚔️  Analyzing competitive intelligence...")
        
        if not self.chat or not self.qa_chain:
            return self._mock_competitive_analysis(transcript_text)
        
        docs = self._create_documents(transcript_text)
        
        try:
            if len(docs) > 1:
                query = "Identify competitive comparisons, market positioning, and strategic advantages mentioned"
                result = self.qa_chain.run(input_documents=docs, question=query)
            else:
                prompt = self.competitive_template.format_prompt(text=transcript_text)
                result = self.chat(prompt.to_messages()).content
            
            return result
        except Exception as e:
            print(f"⚠️  Competitive analysis failed: {e}, using mock analysis")
            return self._mock_competitive_analysis(transcript_text)
    
    def analyze_financial_implications(self, transcript_text: str) -> str:
        """
        Extract financial implications and investment insights
        
        :param transcript_text: Full video transcript
        :return: Financial implications analysis
        """
        print("💰 Analyzing financial implications...")
        
        if not self.chat or not self.qa_chain:
            return self._mock_financial_analysis(transcript_text)
        
        docs = self._create_documents(transcript_text)
        
        try:
            if len(docs) > 1:
                query = "Analyze financial implications, revenue impact, growth prospects, and investment opportunities"
                result = self.qa_chain.run(input_documents=docs, question=query)
            else:
                prompt = self.financial_template.format_prompt(text=transcript_text)
                result = self.chat(prompt.to_messages()).content
            
            return result
        except Exception as e:
            print(f"⚠️  Financial analysis failed: {e}, using mock analysis")
            return self._mock_financial_analysis(transcript_text)
    
    def comprehensive_analysis(self, transcript_text: str) -> AnalysisResult:
        """
        Run complete intelligence analysis pipeline
        
        :param transcript_text: Full video transcript
        :return: Complete analysis results
        """
        import time
        start_time = time.time()
        
        print("🎯 Running comprehensive intelligence analysis...")
        
        # Run all analysis types
        business_insights = self.analyze_business_intelligence(transcript_text)
        entity_analysis = self.analyze_entities_sentiment(transcript_text)
        competitive_intelligence = self.analyze_competitive_intelligence(transcript_text)
        financial_implications = self.analyze_financial_implications(transcript_text)
        
        # Generate summary
        sentiment_summary = self._generate_sentiment_summary(entity_analysis)
        
        processing_time = time.time() - start_time
        
        return AnalysisResult(
            business_insights=business_insights,
            entity_analysis=entity_analysis,
            sentiment_summary=sentiment_summary,
            competitive_intelligence=competitive_intelligence,
            financial_implications=financial_implications,
            processing_time=processing_time
        )
    
    def _generate_sentiment_summary(self, entity_analysis: str) -> str:
        """
        Generate a concise sentiment summary from entity analysis
        
        :param entity_analysis: Entity analysis text
        :return: Sentiment summary
        """
        try:
            prompt = f"""
            Based on this entity analysis, provide a brief sentiment summary:
            
            {entity_analysis}
            
            Summarize in 2-3 sentences:
            1. Overall sentiment tone
            2. Most positive mentions
            3. Most negative mentions
            
            SENTIMENT SUMMARY:
            """
            
            result = self.chat([{"role": "user", "content": prompt}]).content
            return result
        except:
            return "Sentiment summary generation failed"
    
    def query_content(self, transcript_text: str, question: str) -> str:
        """
        Answer specific questions about the video content (RAG-like functionality)
        
        :param transcript_text: Full video transcript
        :param question: Question to answer
        :return: Answer based on transcript content
        """
        print(f"❓ Answering question: {question}")
        
        docs = self._create_documents(transcript_text)
        
        result = self.qa_chain.run(input_documents=docs, question=question)
        return result
    
    async def acomprehensive_analysis(self, transcript_text: str) -> AnalysisResult:
        """
        Async version of comprehensive analysis for better performance
        
        :param transcript_text: Full video transcript
        :return: Complete analysis results
        """
        import time
        start_time = time.time()
        
        print("🚀 Running async comprehensive analysis...")
        
        # Run analyses concurrently
        tasks = [
            asyncio.create_task(self._async_analyze(transcript_text, "business")),
            asyncio.create_task(self._async_analyze(transcript_text, "entity")),
            asyncio.create_task(self._async_analyze(transcript_text, "competitive")),
            asyncio.create_task(self._async_analyze(transcript_text, "financial"))
        ]
        
        results = await asyncio.gather(*tasks)
        
        processing_time = time.time() - start_time
        
        return AnalysisResult(
            business_insights=results[0],
            entity_analysis=results[1],
            sentiment_summary=self._generate_sentiment_summary(results[1]),
            competitive_intelligence=results[2],
            financial_implications=results[3],
            processing_time=processing_time
        )
    
    async def _async_analyze(self, transcript_text: str, analysis_type: str) -> str:
        """
        Async helper for different analysis types
        """
        if analysis_type == "business":
            return self.analyze_business_intelligence(transcript_text)
        elif analysis_type == "entity":
            return self.analyze_entities_sentiment(transcript_text)
        elif analysis_type == "competitive":
            return self.analyze_competitive_intelligence(transcript_text)
        elif analysis_type == "financial":
            return self.analyze_financial_implications(transcript_text)
    
    def _mock_business_analysis(self, transcript_text: str) -> str:
        """Mock business analysis for demo when Claude API is not available"""
        return f"""
🏢 BUSINESS INTELLIGENCE ANALYSIS (DEMO MODE)

📊 MARKET IMPACT ASSESSMENT:
• Video Content: Technology/Product Review Discussion
• Transcript Length: {len(transcript_text):,} characters
• Business Entities: Multiple companies and products mentioned
• Potential Market Relevance: HIGH

💰 KEY BUSINESS INSIGHTS:
• APPLE: Multiple mentions throughout content - mixed sentiment detected
• TECHNOLOGY TRENDS: Discussion of innovation and market positioning
• COMPETITIVE LANDSCAPE: Comparisons between major tech companies
• CONSUMER SENTIMENT: Analysis of product reception and market response

📈 FINANCIAL IMPLICATIONS:
• Stock Impact Potential: Moderate to High for mentioned companies
• Market Timing: Content could influence consumer purchasing decisions
• Risk Factors: Competitive pressure and pricing discussions
• Investment Thesis: Technology adoption trends and market share dynamics

🎯 ACTIONABLE INTELLIGENCE:
• Monitor social media sentiment following video release
• Track stock price movement of mentioned companies
• Assess competitive positioning changes
• Evaluate consumer demand indicators

⚠️  NOTE: This is a demo analysis. Full Claude integration provides detailed, 
specific insights based on actual transcript content analysis.
        """.strip()
    
    def _mock_entity_analysis(self, transcript_text: str) -> str:
        """Mock entity analysis for demo"""
        return """
COMPANIES:
- Apple: +0.3 - Generally positive mentions with some criticism
- Samsung: +0.1 - Neutral to slightly positive competitive references  
- Google: +0.4 - Positive mentions of services and innovation
- Tesla: +0.2 - Mixed discussion of technology and market position

PRODUCTS:
- iPhone: -0.1 - Mixed sentiment with praise and criticism
- Technology Products: +0.5 - Overall positive discussion of innovation

PEOPLE:
- Tech Leaders: +0.2 - Generally respectful discussion of industry figures

KEY TOPICS:
- Innovation: +0.6 - Positive sentiment toward technological advancement
- Market Competition: 0.0 - Neutral analysis of competitive dynamics
- Pricing: -0.3 - Some concern about product pricing strategies
        """
    
    def _mock_competitive_analysis(self, transcript_text: str) -> str:
        """Mock competitive analysis for demo"""
        return """
COMPETITIVE INTELLIGENCE:

🏆 MARKET POSITIONING:
• Apple maintains premium positioning but faces pricing pressure
• Samsung competitive on features and value proposition
• Google strength in AI and services integration

⚔️ COMPETITIVE DYNAMICS:
• Intense competition in smartphone/technology markets
• Innovation cycles driving competitive advantages
• Price-performance ratio becoming key differentiator

📊 STRATEGIC ADVANTAGES:
• Brand loyalty remains strong for established players
• Technology integration creating ecosystem advantages
• Market share battles intensifying across segments

🔍 MARKET OPPORTUNITIES:
• Emerging technology adoption creating new competitive fronts
• Consumer preference shifts opening market gaps
• Global market expansion opportunities for leaders
        """
    
    def _mock_financial_analysis(self, transcript_text: str) -> str:
        """Mock financial analysis for demo"""
        return """
💰 FINANCIAL IMPLICATIONS ANALYSIS:

📈 REVENUE IMPACT:
• Product sentiment could influence quarterly sales
• Competitive positioning affects market share
• Consumer reception impacts demand forecasting

💵 COST CONSIDERATIONS:
• Pricing pressure mentioned multiple times
• R&D investment requirements for competitive position
• Market competition affecting margin sustainability

🎯 INVESTMENT OPPORTUNITIES:
• Technology leaders well-positioned for growth
• Market consolidation creating strategic opportunities
• Innovation cycles presenting timing advantages

⚠️ RISK FACTORS:
• Competitive pressure on pricing and margins
• Market saturation in key segments
• Technology disruption risks for established players

📊 VALUATION INSIGHTS:
• Premium valuations justified by market leadership
• Growth prospects balanced against competitive risks
• Market timing considerations for investment decisions
        """

 
    # Your existing methods...
    
    def comprehensive_analysis_pipeline(self, transcript_data: Dict) -> Dict:
        """Pipeline-optimized comprehensive analysis"""
        
        # Use your existing comprehensive_analysis method
        results = self.comprehensive_analysis(transcript_data['full_text'])
        
        # Add pipeline-specific metadata
        pipeline_results = {
            'video_id': transcript_data['metadata'].video_id,
            'analysis_results': {
                'business_insights': results.business_insights,
                'entity_analysis': results.entity_analysis,
                'sentiment_summary': results.sentiment_summary,
                'competitive_intelligence': results.competitive_intelligence,
                'financial_implications': results.financial_implications
            },
            'processing_metrics': {
                'analysis_time': results.processing_time,
                'model_used': 'claude-3-haiku-20240307',
                'pipeline_timestamp': datetime.now().isoformat()
            },
            'embeddings_ready': True
        }
        
        return pipeline_results
    
    async def batch_analysis(self, transcript_batch: List[Dict]) -> List[Dict]:
        """Async batch analysis for pipeline scale"""
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            tasks = [
                asyncio.get_event_loop().run_in_executor(
                    executor, self.comprehensive_analysis_pipeline, transcript
                )
                for transcript in transcript_batch
            ]
            
            return await asyncio.gather(*tasks, return_exceptions=True)