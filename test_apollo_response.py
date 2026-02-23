"""
Diagnostic script to test Apollo API and see what fields are returned.
"""

import json
from apollo import ApolloClient
from config import load_config

# Load config
config = load_config()

# Initialize Apollo client
client = ApolloClient()

print("Testing Apollo API Response...")
print("=" * 60)

# Test with a sample organization
test_records = [
    {
        "Company Name (Based on Website Domain)": "Anthropic",
        "Website URLs": "anthropic.com"
    }
]

print("\n1. Testing Organization Enrichment")
print("-" * 60)
print(f"Input: {test_records[0]}")

try:
    results = client.enrich_organizations_bulk(test_records)

    if results and len(results) > 0:
        result = results[0]
        print("\n✅ Success! Fields returned by Apollo:")
        print("-" * 60)

        # Separate base and Apollo fields
        base_fields = {k: v for k, v in result.items() if not k.startswith("Apollo")}
        apollo_fields = {k: v for k, v in result.items() if k.startswith("Apollo")}

        print("\n📋 Base Fields:")
        for key, value in base_fields.items():
            if not key.startswith("_"):
                print(f"  {key}: {value}")

        print("\n🚀 Apollo Extra Fields:")
        for key, value in apollo_fields.items():
            print(f"  {key}: {value}")

        # Check for Revenue and Size
        print("\n" + "=" * 60)
        print("📊 Revenue and Size Status:")
        print("-" * 60)

        if result.get("Revenue"):
            print(f"✅ Revenue: {result['Revenue']}")
        else:
            print("❌ Revenue: Not available")
            if result.get("Apollo Company: Revenue Range"):
                print(f"   (But Revenue Range is available: {result['Apollo Company: Revenue Range']})")

        if result.get("Size"):
            print(f"✅ Size: {result['Size']}")
        else:
            print("❌ Size: Not available")
            if result.get("Apollo Company: Employee Range"):
                print(f"   (But Employee Range is available: {result['Apollo Company: Employee Range']})")

        # Save full response to file
        with open('apollo_response_sample.json', 'w') as f:
            json.dump(result, f, indent=2)
        print(f"\n💾 Full response saved to: apollo_response_sample.json")

    else:
        print("❌ No results returned")

except Exception as e:
    print(f"\n❌ Error: {e}")

print("\n" + "=" * 60)
print("Test complete!")
