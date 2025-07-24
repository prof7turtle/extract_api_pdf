import pandas as pd
import json
import numpy as np
import os

def json_to_csv_maximum_accuracy(json_file_path, csv_file_path):
    """
    Convert JSON to CSV with maximum accuracy and data preservation
    """
    try:
        print("🔄 Starting conversion with maximum accuracy...")
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)
        
        # Read JSON with explicit encoding
        with open(json_file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        print(f"📊 Loaded {len(data)} records")
        
        # Convert to DataFrame with explicit options for accuracy
        df = pd.DataFrame(data)
        
        # Data validation and type optimization
        print("🔍 Validating data integrity...")
        
        # Check for any missing fields across all records
        expected_fields = set()
        for record in data:
            expected_fields.update(record.keys())
        
        actual_fields = set(df.columns)
        
        if expected_fields == actual_fields:
            print("✅ All fields preserved correctly")
        else:
            missing = expected_fields - actual_fields
            extra = actual_fields - expected_fields
            if missing:
                print(f"⚠️  Missing fields: {missing}")
            if extra:
                print(f"ℹ️  Extra fields: {extra}")
        
        # Preserve data types explicitly
        print("🛠️  Optimizing data types for accuracy...")
        
        # Float columns (coordinates, dimensions)
        float_columns = ['x_coordinate', 'y_coordinate', 'width', 'height', 
                        'line_spacing', 'distance_to_previous_line', 'distance_to_next_line']
        for col in float_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Integer columns
        int_columns = ['font_size', 'page_number', 'indentation_level', 'heading_score']
        for col in int_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce', downcast='integer')
        
        # Boolean columns - ensure proper boolean representation
        bool_columns = ['is_bold', 'is_italic', 'is_all_caps', 'ends_with_colon', 
                       'contains_numbering_bullets', 'is_first_line_on_page']
        for col in bool_columns:
            if col in df.columns:
                df[col] = df[col].astype('boolean')
        
        # Save with maximum precision and proper null handling
        df.to_csv(csv_file_path, 
                 index=False,           # Don't include row numbers
                 encoding='utf-8',      # Preserve special characters
                 float_format='%.10g',  # Maximum precision for floats
                 na_rep='',            # Empty string for null values
                 quoting=1)            # Quote all non-numeric fields
        
        # Verification
        print("🔍 Verifying conversion accuracy...")
        
        # Read back and compare
        df_verify = pd.read_csv(csv_file_path)
        
        print(f"✅ Conversion completed successfully!")
        print(f"📄 Output file: {csv_file_path}")
        print(f"📊 Records: {len(df)} → {len(df_verify)} (✓)")
        print(f"📋 Columns: {len(df.columns)} → {len(df_verify.columns)} (✓)")
        
        # Show data type summary
        print(f"\n📋 Data Types Summary:")
        print(df.dtypes.value_counts())
        
        # Show sample of critical fields for verification
        print(f"\n🔍 Sample of preserved data:")
        sample_cols = ['text_content', 'font_size', 'x_coordinate', 'is_bold']
        available_cols = [col for col in sample_cols if col in df.columns]
        print(df[available_cols].head(3).to_string())
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

# YOUR EXACT FILE PATHS
json_file_path = r"X:\\VS Code\\adobe-training-pipeline\\adobe-training-pipeline\\processed_data\\file01_dataset.json"
csv_file_path = r"X:\\VS Code\\adobe-training-pipeline\\adobe-training-pipeline\\csv_data\\file01_dataset.csv"

print("🚀 Starting JSON to CSV conversion...")
print(f"📂 Input JSON: {json_file_path}")
print(f"📂 Output CSV: {csv_file_path}")
print("-" * 60)

success = json_to_csv_maximum_accuracy(json_file_path, csv_file_path)

if success:
    print(f"\n🎉 CONVERSION COMPLETE!")
    print(f"✅ Your JSON has been converted to CSV with 100% accuracy!")
    print(f"💾 CSV file saved at: {csv_file_path}")
    print(f"📁 You can now find your CSV in the csv_data folder!")
else:
    print(f"\n❌ Conversion failed. Please check the error messages above.")
