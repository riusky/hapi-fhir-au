#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
HAPI FHIR Custom Search Parameter Registration Tool
"""

import requests
import json
import sys

# Fix Windows console encoding
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass

def register_search_parameter(base_url="http://localhost:19090/fhir"):
    """Register custom search parameter for DocumentReference:content"""
    
    search_param = {
        "resourceType": "SearchParameter",
        "id": "DocumentReference-content",
        "url": "http://hapi-fhir.au/SearchParameter/DocumentReference-content",
        "version": "1.0.0",
        "name": "DocumentReferenceContent",
        "status": "active",
        "experimental": False,
        "date": "2025-12-04",
        "publisher": "HAPI FHIR AU",
        "description": "Reference to DocumentReference content for _include parameter support",
        "code": "content",
        "base": ["DocumentReference"],
        "type": "reference",
        "expression": "DocumentReference.content.attachment",
        "target": ["Binary", "Attachment"]
    }
    
    print("=" * 60)
    print("Registering Custom Search Parameter")
    print("=" * 60)
    print(f"FHIR Server: {base_url}")
    print(f"Search Parameter: DocumentReference:content")
    print()
    
    try:
        # Create/Update search parameter
        url = f"{base_url}/SearchParameter/DocumentReference-content"
        headers = {'Content-Type': 'application/fhir+json'}
        
        print("Creating search parameter...")
        response = requests.put(url, json=search_param, headers=headers, timeout=10)
        
        if response.status_code in [200, 201]:
            print("✓ Search parameter created successfully")
            print(f"  Response: {response.status_code}")
        else:
            print(f"✗ Failed to create search parameter: {response.status_code}")
            print(f"  Response: {response.text}")
            return False
        
        # Trigger reindex
        print("\nTriggering reindex for DocumentReference...")
        reindex_url = f"{base_url}/$reindex"
        reindex_params = {
            "resourceType": "Parameters",
            "parameter": [
                {
                    "name": "url",
                    "valueString": "http://hapi-fhir.au/SearchParameter/DocumentReference-content"
                }
            ]
        }
        
        response = requests.post(reindex_url, json=reindex_params, headers=headers, timeout=30)
        
        if response.status_code in [200, 201, 202]:
            print("✓ Reindex triggered successfully")
        else:
            print(f"⚠ Reindex may have failed: {response.status_code}")
            print(f"  Response: {response.text[:200]}")
        
        # Wait a moment for reindex to start
        print("\nWaiting for reindex to process...")
        import time
        time.sleep(3)
        
        # Verify the search parameter appears in metadata
        print("\nVerifying searchInclude in metadata...")
        metadata_url = f"{base_url}/metadata"
        response = requests.get(metadata_url, timeout=10)
        
        if response.status_code == 200:
            metadata = response.json()
            for resource in metadata.get('rest', [{}])[0].get('resource', []):
                if resource.get('type') == 'DocumentReference':
                    includes = resource.get('searchInclude', [])
                    if 'DocumentReference:content' in includes:
                        print("✓ DocumentReference:content found in searchInclude!")
                    else:
                        print("⚠ DocumentReference:content NOT in searchInclude yet")
                        print(f"  Current includes: {includes[:5]}...")  # Show first 5
                    break
        
        print("\n" + "=" * 60)
        print("Registration Complete!")
        print("=" * 60)
        print("\nYou can now search using:")
        print(f"  GET {base_url}/DocumentReference?content=<search-term>")
        print()
        
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Error: {e}")
        return False

if __name__ == "__main__":
    # Default to exposed port
    base_url = "http://localhost:19090/fhir"
    
    if len(sys.argv) > 1:
        base_url = sys.argv[1]
    
    success = register_search_parameter(base_url)
    sys.exit(0 if success else 1)
