import os, json, zipfile, tempfile, re
from pathlib import Path
from collections import defaultdict
import math

from adobe.pdfservices.operation.auth.service_principal_credentials import ServicePrincipalCredentials
from adobe.pdfservices.operation.pdf_services import PDFServices
from adobe.pdfservices.operation.pdf_services_media_type import PDFServicesMediaType
from adobe.pdfservices.operation.pdfjobs.jobs.extract_pdf_job import ExtractPDFJob
from adobe.pdfservices.operation.pdfjobs.result.extract_pdf_result import ExtractPDFResult
from adobe.pdfservices.operation.pdfjobs.params.extract_pdf.extract_element_type import ExtractElementType
from adobe.pdfservices.operation.pdfjobs.params.extract_pdf.extract_pdf_params import ExtractPDFParams

# Configuration
CRED_PATH = Path(__file__).parent / "pdfservices-api-credentials.json"
RAW_PDFS = Path(__file__).parent.parent / "raw_pdfs"
OUT_DIR = Path(__file__).parent.parent / "processed_data"
OUT_DIR.mkdir(exist_ok=True)

def load_pdfservices(creds_path):
    creds = json.load(open(creds_path))
    spc = ServicePrincipalCredentials(
        client_id=creds["client_credentials"]["client_id"],
        client_secret=creds["client_credentials"]["client_secret"]
    )
    return PDFServices(credentials=spc)

def extract_structured_data(pdf_services, pdf_path: Path):
    """Extract with enhanced document analysis - FIXED API parameters"""
    print(f"  → Processing {pdf_path.name} with document analysis...")
    
    with open(pdf_path, "rb") as f:
        data = f.read()
    
    asset = pdf_services.upload(input_stream=data, mime_type=PDFServicesMediaType.PDF)
    
    # FIXED: Use only valid Adobe PDF Extract API parameters
    try:
        extract_params = ExtractPDFParams(
            elements_to_extract=[ExtractElementType.TEXT]
        )
        job = ExtractPDFJob(input_asset=asset, extract_pdf_params=extract_params)
        print("    → Enhanced extraction parameters applied")
    except Exception as e:
        print(f"    → Using basic extraction: {e}")
        job = ExtractPDFJob(input_asset=asset)
    
    loc = pdf_services.submit(job)
    resp = pdf_services.get_job_result(loc, ExtractPDFResult)
    res_asset = resp.get_result().get_resource()
    sa = pdf_services.get_content(res_asset)
    
    # Extract and analyze ZIP contents
    tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
    tmp.write(sa.get_input_stream())
    tmp.close()
    
    with zipfile.ZipFile(tmp.name, "r") as z:
        print(f"    📁 Files in ZIP: {z.namelist()}")
        raw = z.read("structuredData.json")
    
    os.unlink(tmp.name)
    return json.loads(raw)

def calculate_text_features(elements_by_page):
    """FIXED: Calculate advanced text features with proper error handling for empty sequences"""
    enhanced_elements = []
    
    for page_num, elements in elements_by_page.items():
        if not elements:
            continue
            
        # Sort elements by Y coordinate (top to bottom) - FIXED sorting
        elements.sort(key=lambda x: -x.get('Bounds', [0, 0, 0, 0])[1])
        
        # FIXED: Calculate page-level statistics for better indentation with error handling
        x_coordinates = []
        for elem in elements:
            bounds = elem.get("Bounds", [0, 0, 0, 0])
            if len(bounds) >= 4:
                x_coordinates.append(bounds[0])
        
        # Calculate common left margins and indentation unit with proper error handling
        if x_coordinates:
            sorted_x = sorted(set(x_coordinates))
            left_margin = min(sorted_x)
            
            # FIXED: Calculate indentation unit with proper empty sequence handling
            if len(sorted_x) > 1:
                x_diffs = [sorted_x[i] - sorted_x[i-1] for i in range(1, len(sorted_x))]
                # FIXED: Filter and ensure we have valid differences
                valid_diffs = [diff for diff in x_diffs if diff > 5]
                if valid_diffs:
                    indentation_unit = min(valid_diffs)
                else:
                    # No valid differences found, use default
                    indentation_unit = 20
            else:
                indentation_unit = 20
        else:
            left_margin = 0
            indentation_unit = 20
        
        for i, elem in enumerate(elements):
            text = elem.get("Text", "").strip()
            if not text or len(text) < 2:
                continue
                
            bounds = elem.get("Bounds", [0, 0, 0, 0])
            if len(bounds) < 4:
                continue
                
            x, y, x2, y2 = bounds
            
            # FIXED: Calculate distances with improved logic
            prev_distance = None
            next_distance = None
            
            # Calculate distance to previous element
            if i > 0:
                for j in range(i-1, -1, -1):  # Look backward for valid element
                    prev_elem = elements[j]
                    prev_text = prev_elem.get("Text", "").strip()
                    prev_bounds = prev_elem.get("Bounds", [0, 0, 0, 0])
                    
                    if prev_text and len(prev_text) >= 2 and len(prev_bounds) >= 4:
                        prev_y = prev_bounds[1]
                        prev_distance = abs(prev_y - y)
                        break
            
            # Calculate distance to next element
            if i < len(elements) - 1:
                for j in range(i+1, len(elements)):  # Look forward for valid element
                    next_elem = elements[j]
                    next_text = next_elem.get("Text", "").strip()
                    next_bounds = next_elem.get("Bounds", [0, 0, 0, 0])
                    
                    if next_text and len(next_text) >= 2 and len(next_bounds) >= 4:
                        next_y = next_bounds[1]
                        next_distance = abs(y - next_y)
                        break
            
            # FIXED: Improved indentation calculation with error handling
            try:
                indentation_level = max(0, round((x - left_margin) / indentation_unit))
            except (ZeroDivisionError, ValueError):
                indentation_level = 0
            
            # FIXED: Better line spacing calculation
            # For first element on page, use a reasonable default or calculate from next element
            if prev_distance is None and i == 0:
                # First element on page - use distance to next element or default
                line_spacing = next_distance if next_distance is not None else 0
            else:
                line_spacing = prev_distance if prev_distance is not None else 0
            
            # Enhanced element with calculated features
            enhanced_elem = elem.copy()
            enhanced_elem.update({
                'distance_to_previous_line': prev_distance,
                'distance_to_next_line': next_distance,
                'line_spacing': line_spacing,  # FIXED: Proper line spacing
                'indentation_level': indentation_level,  # FIXED: Improved indentation
                'is_first_line_on_page': (i == 0),
                'computed_x': x,
                'computed_y': y,
                'computed_width': x2 - x,
                'computed_height': y2 - y,
                'page_left_margin': left_margin,  # Additional context
                'page_indentation_unit': indentation_unit  # Additional context
            })
            
            enhanced_elements.append(enhanced_elem)
    
    return enhanced_elements

def build_comprehensive_dataset(adobe_data, pdf_name):
    """Build dataset with all required features - FIXED spacing calculations"""
    elems = adobe_data.get("elements", [])
    print(f"\n🔍 COMPREHENSIVE ANALYSIS for {pdf_name}:")
    print(f"  📊 Total elements: {len(elems)}")
    
    if not elems:
        return []
    
    # Group elements by page
    elements_by_page = defaultdict(list)
    for elem in elems:
        page = elem.get("Page", 1)
        elements_by_page[page].append(elem)
    
    # Calculate advanced features with FIXED spacing logic
    enhanced_elements = calculate_text_features(elements_by_page)
    
    # Analyze font patterns
    font_sizes = []
    text_lengths = []
    
    for elem in enhanced_elements:
        # Extract font info from multiple possible locations
        font_size = 12  # default
        
        # Check different paths for font size
        if "Font" in elem:
            font_size = elem["Font"].get("size", font_size)
        elif "TextStyle" in elem:
            font_size = elem["TextStyle"].get("FontSize", font_size)
        elif "Style" in elem:
            font_size = elem["Style"].get("size", elem["Style"].get("FontSize", font_size))
        
        if font_size > 0:
            font_sizes.append(font_size)
        
        text = elem.get("Text", "").strip()
        text_lengths.append(len(text))
    
    if not font_sizes:
        print("  ❌ No font information found")
        return []
    
    # Calculate statistics
    avg_font_size = sum(font_sizes) / len(font_sizes)
    max_font_size = max(font_sizes)
    unique_sizes = sorted(set(font_sizes), reverse=True)
    avg_text_length = sum(text_lengths) / len(text_lengths)
    
    print(f"  📐 Font sizes found: {unique_sizes}")
    print(f"  📐 Average font: {avg_font_size:.1f}, Max: {max_font_size}")
    print(f"  📝 Average text length: {avg_text_length:.1f}")
    
    # Adaptive thresholds based on document characteristics
    if len(unique_sizes) == 1:
        # All text has same font size - use other heuristics
        print("  🎯 Uniform font size detected - using content-based detection")
        size_threshold = unique_sizes[0]  # Use the single font size
    else:
        # Multiple font sizes - use size-based detection
        size_threshold = avg_font_size * 1.1
    
    features = []
    title_found = False
    
    for elem in enhanced_elements:
        text = elem.get("Text", "").strip()
        
        # Skip very short, very long, or empty text
        if len(text) < 3 or len(text) > 200:
            continue
        
        # Extract font information with robust handling
        font_size = 12
        font_name = ""
        is_bold = False
        is_italic = False
        
        # Try multiple paths for font information
        font_info = None
        if "Font" in elem:
            font_info = elem["Font"]
        elif "TextStyle" in elem:
            font_info = elem["TextStyle"]
        elif "Style" in elem:
            font_info = elem["Style"]
        
        if font_info:
            font_size = font_info.get("size", font_info.get("FontSize", 12))
            font_name = str(font_info.get("name", font_info.get("FontName", "")))
            
            # Handle font weight (numeric or string)
            weight = font_info.get("weight", font_info.get("FontWeight", ""))
            if isinstance(weight, (int, float)):
                is_bold = weight >= 600
            elif isinstance(weight, str):
                is_bold = "bold" in weight.lower()
            
            # Handle font style
            style = font_info.get("style", font_info.get("FontStyle", ""))
            if isinstance(style, str):
                is_italic = "italic" in style.lower()
        
        # Multi-criteria heading detection
        heading_score = 0
        
        # Criterion 1: Font size (if varied)
        if len(unique_sizes) > 1 and font_size >= size_threshold:
            heading_score += 2
        
        # Criterion 2: Bold formatting
        if is_bold:
            heading_score += 2
        
        # Criterion 3: Short text (likely title/heading)
        word_count = len(text.split())
        if 2 <= word_count <= 10:
            heading_score += 1
        
        # Criterion 4: All caps
        if text.isupper() and len(text) > 4:
            heading_score += 1
        
        # Criterion 5: Ends with colon
        if text.endswith(':'):
            heading_score += 1
        
        # Criterion 6: Contains numbering
        if re.match(r'^\d+\.?\d*\.?\s+', text):
            heading_score += 2
        
        # Criterion 7: Common heading words
        heading_words = ['introduction', 'conclusion', 'summary', 'overview', 'background', 
                        'methodology', 'results', 'discussion', 'references', 'abstract',
                        'chapter', 'section', 'part', 'appendix', 'goals', 'mission',
                        'history', 'revision', 'table', 'contents', 'acknowledgements']
        if any(word in text.lower() for word in heading_words):
            heading_score += 1
        
        # Criterion 8: Position and spacing
        if elem.get('is_first_line_on_page', False):
            heading_score += 1
        
        # FIXED: Safe comparison with None values and improved logic
        prev_distance = elem.get('distance_to_previous_line')
        if prev_distance is not None and prev_distance > 20:  # Large space before
            heading_score += 1
        
        # Criterion 9: Standalone lines (common for headings)
        if word_count <= 8 and not any(word in text.lower() for word in ['the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by']):
            heading_score += 1
        
        # Determine if this is a heading based on score
        is_heading_candidate = heading_score >= 2  # Lowered threshold for uniform fonts
        
        if is_heading_candidate:
            # Determine heading level
            label = None
            
            # Title detection
            if not title_found and (heading_score >= 4 or 
                                   any(word in text.lower() for word in ['overview', 'guide', 'manual', 'report', 'application', 'form'])):
                label = "title"
                title_found = True
            # H1 detection
            elif heading_score >= 3 or re.match(r'^\d+\.\s+', text):
                label = "H1"
            # H2 detection
            elif heading_score >= 2 or re.match(r'^\d+\.\d+\s+', text):
                label = "H2"
            # H3 detection
            else:
                label = "H3"
            
            if label:
                # FIXED: Build complete feature set with proper spacing values
                feature = {
                    "text_content": text,
                    "font_size": font_size,
                    "font_name": font_name,
                    "is_bold": is_bold,
                    "is_italic": is_italic,
                    "is_all_caps": text.isupper() and len(text) > 1,
                    "x_coordinate": elem.get('computed_x', 0),
                    "y_coordinate": elem.get('computed_y', 0),
                    "width": elem.get('computed_width', 0),
                    "height": elem.get('computed_height', 0),
                    "page_number": elem.get("Page", 1) + 1,  # FIXED: 1-indexed pages
                    "line_spacing": elem.get('line_spacing', 0),  # FIXED: Always has a value
                    "indentation_level": elem.get('indentation_level', 0),  # FIXED: Improved calculation
                    "ends_with_colon": text.endswith(":"),
                    "contains_numbering_bullets": bool(re.match(r"^(\d+[\.\)]|\-|\•|\*)\s+", text)),
                    "is_first_line_on_page": elem.get('is_first_line_on_page', False),
                    "distance_to_previous_line": elem.get('distance_to_previous_line'),  # FIXED: Keep None for first elements
                    "distance_to_next_line": elem.get('distance_to_next_line'),  # FIXED: Keep None where appropriate
                    "label": label,
                    "heading_score": heading_score
                }
                
                features.append(feature)
                print(f"    📝 Found {label}: '{text[:40]}...' (score: {heading_score}, spacing: {feature['line_spacing']}, indent: {feature['indentation_level']})")
    
    return features

def main():
    ps = load_pdfservices(CRED_PATH)
    print("✅ Adobe PDF Services ready")
    
    total_features = 0
    
    for pdf in sorted(RAW_PDFS.glob("*.pdf")):
        print(f"\n📄 Processing {pdf.name}")
        try:
            data = extract_structured_data(ps, pdf)
            feats = build_comprehensive_dataset(data, pdf.name)
            
            out = OUT_DIR / f"{pdf.stem}_dataset.json"
            with open(out, "w", encoding="utf-8") as f:
                json.dump(feats, f, indent=2, ensure_ascii=False)
            
            total_features += len(feats)
            print(f"  ✅ {len(feats)} heading/title rows → {out.name}")
            
            # Show detailed samples
            if feats:
                for feat in feats[:3]:  # Show first 3
                    print(f"    • {feat['label']}: '{feat['text_content'][:30]}...'")
                    print(f"      Font: {feat['font_size']}, Bold: {feat['is_bold']}, Spacing: {feat['line_spacing']}, Indent: {feat['indentation_level']}")
        
        except Exception as e:
            print(f"  ❌ Error: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n📊 Total features extracted: {total_features}")

if __name__ == "__main__":
    main()
