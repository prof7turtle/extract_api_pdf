import pandas as pd
import json
import os
from pathlib import Path

def combine_all_json_to_csv(input_folder, output_csv_path):
    """
    Combine all JSON files from input folder into a single CSV
    """
    try:
        print("🔄 Combining all JSON files into one CSV...")
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
        
        # Find all JSON files in the folder
        input_path = Path(input_folder)
        json_files = list(input_path.glob("*.json"))
        
        if not json_files:
            print(f"❌ No JSON files found in {input_folder}")
            return False
        
        print(f"📁 Found {len(json_files)} JSON files")
        
        # Combine all data from all JSON files
        all_data = []
        total_records = 0
        
        for json_file in json_files:
            print(f"📖 Reading {json_file.name}...")
            
            with open(json_file, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            records_count = len(data)
            all_data.extend(data)
            total_records += records_count
            
            print(f"   ✅ Added {records_count} records")
        
        print(f"\n📊 Total combined records: {total_records}")
        
        # Convert to DataFrame
        df = pd.DataFrame(all_data)
        
        # Replace null values with 0
        print("🔄 Replacing null values with 0...")
        df = df.fillna(0)
        
        # Optimize data types for accuracy
        print("🛠️ Optimizing data types...")
        
        # Float columns
        float_columns = ['x_coordinate', 'y_coordinate', 'width', 'height', 
                        'line_spacing', 'distance_to_previous_line', 'distance_to_next_line']
        for col in float_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Integer columns
        int_columns = ['font_size', 'page_number', 'indentation_level', 'heading_score']
        for col in int_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
        
        # Boolean columns
        bool_columns = ['is_bold', 'is_italic', 'is_all_caps', 'ends_with_colon', 
                       'contains_numbering_bullets', 'is_first_line_on_page']
        for col in bool_columns:
            if col in df.columns:
                df[col] = df[col].fillna(False).astype('bool')
        
        # Final cleanup - ensure no nulls remain
        df = df.fillna(0)
        
        # Save to CSV with maximum accuracy
        df.to_csv(output_csv_path, 
                 index=False,           # No row numbers
                 encoding='utf-8',      # Preserve special characters
                 float_format='%.10g',  # Maximum precision for floats
                 na_rep='0')           # Replace any remaining nulls with 0
        
        # Verification
        print("🔍 Verifying final CSV...")
        df_verify = pd.read_csv(output_csv_path)
        null_count = df_verify.isnull().sum().sum()
        
        print(f"\n✅ SUCCESS! CSV created successfully!")
        print(f"📄 Output file: {output_csv_path}")
        print(f"📊 Total records: {len(df_verify)}")
        print(f"📋 Total columns: {len(df_verify.columns)}")
        print(f"🔢 Null values in CSV: {null_count}")
        
        # Show sample data
        print(f"\n🔍 Sample of first 3 rows:")
        sample_cols = ['text_content', 'font_size', 'x_coordinate', 'is_bold']
        available_cols = [col for col in sample_cols if col in df_verify.columns]
        print(df_verify[available_cols].head(3).to_string())
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

# YOUR PATHS
input_folder = r"X:\\VS Code\\adobe-training-pipeline\\adobe-training-pipeline\\processed_data"
output_csv_path = r"X:\\VS Code\\adobe-training-pipeline\\adobe-training-pipeline\\csv_data\\combined_all_data.csv"

print("🚀 Starting JSON to CSV combination...")
print(f"📂 Input folder: {input_folder}")
print(f"📂 Output CSV: {output_csv_path}")
print("-" * 60)

success = combine_all_json_to_csv(input_folder, output_csv_path)

if success:
    print(f"\n🎉 DONE!")
    print(f"✅ All 5 JSON files combined into one CSV!")
    print(f"💾 Find your CSV at: {output_csv_path}")
else:
    print(f"\n❌ Something went wrong. Check the errors above.")
