"""
Debug script to check Excel parsing and domain extraction.
"""

import sys
import pandas as pd
from ingest import ExcelIngestor
from db import DatabaseManager
from apollo import ApolloClient

if len(sys.argv) < 2:
    print("Usage: python debug_excel.py <path_to_excel_file>")
    sys.exit(1)

excel_path = sys.argv[1]

print("=" * 80)
print("EXCEL FILE DIAGNOSTIC")
print("=" * 80)

# Initialize components
db = DatabaseManager()
apollo = ApolloClient()
ingestor = ExcelIngestor(db, apollo)

print("\n1. READING EXCEL FILE")
print("-" * 80)
try:
    df_raw = pd.read_excel(excel_path)
    print(f"✅ File read successfully")
    print(f"   Rows: {len(df_raw)}")
    print(f"   Columns: {list(df_raw.columns)}")
except Exception as e:
    print(f"❌ Error reading file: {e}")
    sys.exit(1)

print("\n2. COLUMN MAPPING")
print("-" * 80)
df_mapped = ingestor._detect_and_map_columns(df_raw)
print(f"Columns after mapping: {list(df_mapped.columns)}")

if "Website URLs" in df_mapped.columns:
    print(f"✅ 'Website URLs' column found")
    print(f"\nFirst 5 values:")
    for i, val in enumerate(df_mapped["Website URLs"].head(5)):
        print(f"   Row {i+1}: '{val}' (type: {type(val).__name__})")
else:
    print(f"❌ 'Website URLs' column NOT found after mapping")

print("\n3. NORMALIZATION (Domain Extraction)")
print("-" * 80)
df_normalized = ingestor.normalize_dataframe(df_mapped)

if "Website URLs" in df_normalized.columns:
    print(f"Domains after normalization:")
    for i, val in enumerate(df_normalized["Website URLs"].head(10)):
        is_empty = not val or str(val).strip() == "" or str(val).lower() == "nan"
        status = "❌ EMPTY" if is_empty else "✅ Valid"
        print(f"   Row {i+1}: '{val}' {status}")

    # Count empty vs valid
    valid_count = sum(1 for val in df_normalized["Website URLs"]
                     if val and str(val).strip() and str(val).lower() != "nan")
    empty_count = len(df_normalized) - valid_count

    print(f"\n📊 Summary:")
    print(f"   Total rows: {len(df_normalized)}")
    print(f"   Valid domains: {valid_count}")
    print(f"   Empty/missing domains: {empty_count}")

print("\n4. RECORDS THAT WILL BE SENT TO APOLLO")
print("-" * 80)

records = df_normalized.to_dict('records')
records_with_domain = []
records_without_domain = []

for record in records[:10]:  # Check first 10
    has_domain = record.get("Website URLs") and str(record["Website URLs"]).strip()
    has_company = record.get("Company Name (Based on Website Domain)") and str(record["Company Name (Based on Website Domain)"]).strip()

    if has_domain or has_company:
        records_with_domain.append(record)
        print(f"✅ WILL ENRICH: Domain='{record.get('Website URLs')}', Company='{record.get('Company Name (Based on Website Domain)')}'")
    else:
        records_without_domain.append(record)
        print(f"❌ SKIP ENRICH: Domain='{record.get('Website URLs')}', Company='{record.get('Company Name (Based on Website Domain)')}'")

print(f"\n📊 First 10 records:")
print(f"   Will enrich: {len(records_with_domain)}")
print(f"   Will skip: {len(records_without_domain)}")

print("\n5. ORGANIZATION PAYLOAD TEST")
print("-" * 80)
if records_with_domain:
    payload = apollo._prepare_org_payload(records_with_domain[:5])
    print(f"Payload for first 5 records with domain:")
    import json
    print(json.dumps(payload, indent=2))
else:
    print("❌ No records with domain/company info to create payload")

print("\n" + "=" * 80)
print("DIAGNOSTIC COMPLETE")
print("=" * 80)

db.close()
