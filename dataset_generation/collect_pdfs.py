import requests
from pathlib import Path
import time

class PDFCollector:
    def __init__(self, output_dir: str = "../raw_pdfs"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        print(f"PDFs will be saved to: {self.output_dir.absolute()}")
    
    def collect_diverse_samples(self):
        """Collect diverse PDF samples for training"""
        
        # Sample PDFs for training (you can add more)
        pdf_sources = [
            {
                "url": "https://arxiv.org/pdf/2301.07041.pdf",
                "name": "academic_paper_1.pdf",
                "type": "Academic Paper"
            },
            {
                "url": "https://arxiv.org/pdf/2106.09685.pdf", 
                "name": "academic_paper_2.pdf",
                "type": "Academic Paper"
            },
            {
                "url": "https://arxiv.org/pdf/2203.15556.pdf",
                "name": "academic_paper_3.pdf", 
                "type": "Academic Paper"
            }
        ]
        
        print("🔄 Collecting diverse PDF samples for training...")
        print(f"Target: {len(pdf_sources)} PDFs")
        
        success_count = 0
        
        for source in pdf_sources:
            try:
                print(f"\n📥 Downloading: {source['name']} ({source['type']})")
                
                response = requests.get(source['url'], timeout=60)
                response.raise_for_status()
                
                output_path = self.output_dir / source['name']
                
                with open(output_path, 'wb') as f:
                    f.write(response.content)
                
                file_size = output_path.stat().st_size / (1024 * 1024)  # MB
                print(f"✅ Downloaded: {source['name']} ({file_size:.1f} MB)")
                success_count += 1
                
                time.sleep(2)  # Be respectful to servers
                
            except Exception as e:
                print(f"❌ Failed to download {source['name']}: {e}")
        
        print(f"\n📊 Collection Summary:")
        print(f"Successfully downloaded: {success_count}/{len(pdf_sources)} PDFs")
        print(f"Saved to: {self.output_dir.absolute()}")
        
        return success_count

if __name__ == "__main__":
    collector = PDFCollector()
    success_count = collector.collect_diverse_samples()
    
    if success_count > 0:
        print(f"\n🎯 Ready for next step: Adobe API processing")
    else:
        print(f"\n⚠️ No PDFs downloaded. Check internet connection.")
