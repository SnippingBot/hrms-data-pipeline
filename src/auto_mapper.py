import pandas as pd
import json
import sys
import os
import google.generativeai as genai

# Configure your API key here
API_KEY = "Your API Key" 
genai.configure(api_key=API_KEY)

def extract_headers_from_excel(excel_path):
    # Skip the first 3 rows of metadata so Row 4 becomes the header
    df = pd.read_excel(excel_path, skiprows=3, nrows=0)
    return df.columns.tolist()

def generate_mapping(system_name, file_path, headers):
    # Using the newer standard model name to avoid deprecation warnings
    model = genai.GenerativeModel('gemini-3.5-flash-lite')
    
    prompt = f"""
    You are an expert Data Engineer. I need to map source headers to a specific target schema.
    
    Source System: {system_name}
    Source File Path: {file_path}
    Source Headers: {headers}
    
    Target Tables & Fields:
    1. 'employee': source_employee_id (string), full_name (string), email (string), hire_date (date), grade (string), department (string), location (string), country (string), gender (string), manager_employee_id (string), employment_status (string)
    2. 'salary': source_employee_id (string), annual_base_local (decimal), currency (string)
    
    Transformation Rules (Use PySpark SQL in 'expr'):
    - 'hire_date': COALESCE(TO_DATE(col, 'MM-dd-yyyy'), TO_DATE(col, 'yyyy-MM-dd'), TO_DATE(col, 'dd-MM-yyyy'))
    - 'gender': LEFT(UPPER(TRIM(col)), 1)
    - 'email': LOWER(TRIM(col))
    - 'employment_status': CASE WHEN UPPER(TRIM(col)) IN ('A', 'ACTIVE') THEN 'Active' WHEN UPPER(TRIM(col)) IN ('T', 'TERMINATED', 'SEPARATED') THEN 'Terminated' ELSE 'Unknown' END
    - 'manager_employee_id': NULLIF(TRIM(CAST(col AS STRING)), '0')
    - 'annual_base_local': CAST(col AS DECIMAL(18,2)). If the source indicates monthly pay, multiply by 12.
    - If the source has separate first/last names, use TRIM(CONCAT()). All IDs must be CAST to STRING.
    
    Output ONLY a valid JSON object matching this exact structure, with no markdown tags or code blocks. The validation_rules array MUST be exactly as shown below:
    {{
      "source_system": "{system_name}",
      "source_file": "{file_path}",
      "mappings": [
        {{ "target_table": "employee", "target_field": "...", "expr": "..." }}
      ],
      "validation_rules": [
        "source_employee_id IS NOT NULL",
        "full_name IS NOT NULL",
        "hire_date IS NOT NULL",
        "hire_date <= CURRENT_DATE()",
        "grade IS NOT NULL",
        "employment_status IN ('Active', 'Terminated')",
        "annual_base_local IS NOT NULL",
        "annual_base_local > 0",
        "currency IS NOT NULL",
        "currency IN ('INR', 'AED', 'USD', 'GBP')"
      ]
    }}
    """
    
    print(f"Calling LLM to generate mapping for {system_name}...")
    response = model.generate_content(prompt)
    
    raw_text = response.text.strip()
    if raw_text.startswith("```json"):
        raw_text = raw_text[7:]
    if raw_text.startswith("```"):
        raw_text = raw_text[3:]
    if raw_text.endswith("```"):
        raw_text = raw_text[:-3]
        
    return json.loads(raw_text.strip())

def main(file_path, system_name, output_json_path):
    headers = extract_headers_from_excel(file_path)
    print(f"Extracted headers: {headers}")
    
    mapping_json = generate_mapping(system_name, file_path, headers)
    
    with open(output_json_path, 'w', encoding='utf-8') as f:
        json.dump(mapping_json, f, indent=2)
        
    print(f"Successfully generated mapping file at: {output_json_path}")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python src/auto_mapper.py <path_to_excel> <system_name> <output_json_path>")
        sys.exit(1)
    
    main(sys.argv[1], sys.argv[2], sys.argv[3])