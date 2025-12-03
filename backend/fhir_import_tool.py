#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
FHIR Resource Import Tool
Batch import FHIR medical document resources to HAPI server
"""

import os
import json
import requests
import sys
from typing import Dict, List, Any


class FHIRImporter:
    """FHIR Resource Importer"""
    
    def __init__(self, base_url: str = "http://localhost:9090/fhir"):
        """
        Initialize the importer
        
        Args:
            base_url: Base URL of FHIR server
        """
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/fhir+json',
            'Accept': 'application/fhir+json'
        })
        
        # Resource import order (sorted by dependencies)
        # Note: CarePathPlan corresponds to file CarePathPlan.json
        self.import_order = [
            'Patient',
            'DocumentReference',
            'CarePathPlan',
            'ServiceRequest',
            'Task'
        ]
        
        # Statistics
        self.stats = {
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'total': 0
        }
    
    def get_missing_resources(self):
        """Get missing dependency resource definitions"""
        return [
            # Practitioners
            {
                "resourceType": "Practitioner",
                "id": "Practitioner-1327",
                "name": [{"text": "Yan***. Park", "family": "Park", "given": ["Yan***."]}],
                "active": True
            },
            {
                "resourceType": "Practitioner",
                "id": "Practitioner-1327111",
                "name": [{"text": "Yan***. ****k2", "family": "****k2", "given": ["Yan***"]}],
                "active": True
            },
            {
                "resourceType": "Practitioner",
                "id": "Practitioner-13271",
                "name": [{"text": "Yan***. ***22k", "family": "***22k", "given": ["Yan***."]}],
                "active": True
            },
            {
                "resourceType": "Practitioner",
                "id": "Practitioner-13278",
                "name": [{"text": "User. Test", "family": "Test", "given": ["User"]}],
                "active": True
            },
            {
                "resourceType": "Practitioner",
                "id": "Practitioner-1580",
                "name": [{"text": "RO Drtest ****", "family": "Drtest", "given": ["RO"]}],
                "active": True
            },
            {
                "resourceType": "Practitioner",
                "id": "Practitioner-1635",
                "name": [{"text": "Justin V****", "family": "Visak", "given": ["Justin"]}],
                "active": True
            },
            {
                "resourceType": "Practitioner",
                "id": "Practitioner-1722",
                "name": [{"text": "Yen-Peng ****", "family": "Liao", "given": ["Yen-Peng"]}],
                "active": True
            },
            {
                "resourceType": "Practitioner",
                "id": "Practitioner-1766",
                "name": [{"text": "Sagar Ghi****", "family": "Ghimire", "given": ["Sagar"]}],
                "active": True
            },
            {
                "resourceType": "Practitioner",
                "id": "Practitioner-1738",
                "name": [{"text": "Unknown Practit****", "family": "Unknown", "given": ["Practitioner"]}],
                "active": True
            },
            # Organizations
            {
                "resourceType": "Organization",
                "id": "Organization-Prov-3",
                "name": "UT SOUTHWESTERN",
                "active": True
            },
            {
                "resourceType": "Organization",
                "id": "Organization-Dept-3",
                "name": "Radiation Oncology",
                "active": True
            },
            # ActivityDefinitions
            {"resourceType": "ActivityDefinition", "id": "ActivityDefinition-1398", "status": "active", "name": "Replan"},
            {"resourceType": "ActivityDefinition", "id": "ActivityDefinition-1429", "status": "active", "name": "Start Date - STANDARD"},
            {"resourceType": "ActivityDefinition", "id": "ActivityDefinition-1434", "status": "active", "name": "Dosi Chart Review"},
            {"resourceType": "ActivityDefinition", "id": "ActivityDefinition-1486", "status": "active", "name": "Unity Dry Run"},
            {"resourceType": "ActivityDefinition", "id": "ActivityDefinition-1489", "status": "active", "name": "Unity-Phys Tx Approve"},
            {"resourceType": "ActivityDefinition", "id": "ActivityDefinition-1388", "status": "active", "name": "Import Images/Fusion"},
            {"resourceType": "ActivityDefinition", "id": "ActivityDefinition-1390", "status": "active", "name": "Generate Contours"},
            {"resourceType": "ActivityDefinition", "id": "ActivityDefinition-1391", "status": "active", "name": "Treatment Planning"},
            {"resourceType": "ActivityDefinition", "id": "ActivityDefinition-1392", "status": "active", "name": "Review Plan(s)"},
            {"resourceType": "ActivityDefinition", "id": "ActivityDefinition-1393", "status": "active", "name": "Chart Preparation"},
            {"resourceType": "ActivityDefinition", "id": "ActivityDefinition-1397", "status": "active", "name": "Sign Plan Doc & Rx"},
            {"resourceType": "ActivityDefinition", "id": "ActivityDefinition-1412", "status": "active", "name": "Review Import/Add Care Path"},
            {"resourceType": "ActivityDefinition", "id": "ActivityDefinition-1451", "status": "active", "name": "3D/IMRT Planning Note"},
            {"resourceType": "ActivityDefinition", "id": "ActivityDefinition-1465", "status": "active", "name": "Sign Mosaiq Rx"},
            {"resourceType": "ActivityDefinition", "id": "ActivityDefinition-1482", "status": "active", "name": "Unity Plan QA"},
            {"resourceType": "ActivityDefinition", "id": "ActivityDefinition-1484", "status": "active", "name": "Unity Physics Initial Chart Check"},
            # Groups
            {"resourceType": "Group", "id": "Group-10006", "type": "practitioner", "actual": True, "name": "DosiChartCheck"},
            {"resourceType": "Group", "id": "Group-10004", "type": "practitioner", "actual": True, "name": "Physics-Ethos"},
            {"resourceType": "Group", "id": "Group-74", "type": "practitioner", "actual": True, "name": "Physician"},
            {"resourceType": "Group", "id": "Group-10010", "type": "practitioner", "actual": True, "name": "Physics-MPA"},
            {"resourceType": "Group", "id": "Group-64", "type": "practitioner", "actual": True, "name": "Radiation Therapists"},
            {"resourceType": "Group", "id": "Group-10007", "type": "practitioner", "actual": True, "name": "Physics-Unity"},
            {"resourceType": "Group", "id": "Group-52", "type": "practitioner", "actual": True, "name": "Dosimetry"},
            {"resourceType": "Group", "id": "Group-10008", "type": "practitioner", "actual": True, "name": "Physics-Treatment"},
        ]
    
    def get_patient_dependent_resources(self):
        """Get resources that depend on Patient (need to be created after Patient import)"""
        return [
            # ServiceRequests that depend on Patient
            {"resourceType": "ServiceRequest", "id": "ActivityInstance-426538", "status": "draft", "intent": "order", "code": {"text": "Placeholder ServiceRequest"}, "subject": {"reference": "Patient/Patient-29590"}},
            {"resourceType": "ServiceRequest", "id": "ActivityInstance-426539", "status": "draft", "intent": "order", "code": {"text": "Placeholder ServiceRequest"}, "subject": {"reference": "Patient/Patient-29590"}},
            {"resourceType": "ServiceRequest", "id": "ActivityInstance-426540", "status": "draft", "intent": "order", "code": {"text": "Placeholder ServiceRequest"}, "subject": {"reference": "Patient/Patient-29590"}},
            {"resourceType": "ServiceRequest", "id": "ActivityInstance-426541", "status": "draft", "intent": "order", "code": {"text": "Placeholder ServiceRequest"}, "subject": {"reference": "Patient/Patient-29590"}},
        ]
    
    def create_patient_dependent_resources(self) -> None:
        """Create resources that depend on Patient"""
        print("\nCreating Patient-dependent resources...")
        
        patient_resources = self.get_patient_dependent_resources()
        success = 0
        
        for resource in patient_resources:
            resource_type = resource['resourceType']
            resource_id = resource['id']
            
            try:
                url = f"{self.base_url}/{resource_type}/{resource_id}"
                response = self.session.put(url, json=resource, timeout=10)
                
                if response.status_code in [200, 201]:
                    success += 1
            except:
                pass
        
        if success > 0:
            print(f"Created: {success} Patient-dependent resources")
    
    def create_missing_resources(self) -> None:
        """Create missing dependency resources"""
        print("\n" + "=" * 60)
        print("Creating dependency resources (if missing)")
        print("=" * 60)
        
        missing_resources = self.get_missing_resources()
        success = 0
        failed = 0
        failed_resources = []
        
        for resource in missing_resources:
            resource_type = resource['resourceType']
            resource_id = resource['id']
            
            try:
                url = f"{self.base_url}/{resource_type}/{resource_id}"
                response = self.session.put(url, json=resource, timeout=10)
                
                if response.status_code in [200, 201]:
                    success += 1
                else:
                    failed += 1
                    error_info = f"{resource_type}/{resource_id} (HTTP {response.status_code})"
                    failed_resources.append(error_info)
                    # Print detailed error information
                    try:
                        error_detail = response.json()
                        if 'issue' in error_detail:
                            for issue in error_detail['issue']:
                                diagnostics = issue.get('diagnostics', '')
                                print(f"  ✗ {resource_type}/{resource_id}: {diagnostics}")
                    except:
                        print(f"  ✗ {resource_type}/{resource_id}: HTTP {response.status_code}")
            except Exception as e:
                failed += 1
                failed_resources.append(f"{resource_type}/{resource_id} ({str(e)})")
                print(f"  ✗ {resource_type}/{resource_id}: {e}")
        
        print(f"Dependency resource creation completed: Success {success}, Failed {failed}")
        
        if failed > 0:
            print("\n⚠ Warning: Some dependency resources failed to create:")
            for res in failed_resources:
                print(f"  - {res}")
            print("\nContinuing with main data import, reference errors may occur...")
        
        print("=" * 60)
    
    def check_server_connection(self) -> bool:
        """
        Check server connection
        
        Returns:
            bool: Whether connection is successful
        """
        try:
            response = self.session.get(f"{self.base_url}/metadata", timeout=5)
            if response.status_code == 200:
                print(f"✓ Successfully connected to FHIR server: {self.base_url}")
                return True
            else:
                print(f"✗ Server responded with error: {response.status_code}")
                return False
        except requests.exceptions.RequestException as e:
            print(f"✗ Unable to connect to FHIR server: {e}")
            return False
    
    def load_mock_data(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Load mock data file
        
        Args:
            file_path: JSON file path
            
        Returns:
            List[Dict]: Resource list
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            # Check data structure
            if isinstance(data, dict) and 'data' in data:
                return data['data']
            elif isinstance(data, list):
                return data
            elif isinstance(data, dict) and 'resourceType' in data:
                return [data]
            else:
                print(f"⚠ Unknown data format: {file_path}")
                return []
        except Exception as e:
            print(f"✗ Failed to read file {file_path}: {e}")
            return []
    
    def import_resource(self, resource: Dict[str, Any]) -> bool:
        """
        Import a single FHIR resource
        
        Args:
            resource: FHIR resource object
            
        Returns:
            bool: Whether import was successful
        """
        resource_type = resource.get('resourceType')
        resource_id = resource.get('id')
        
        if not resource_type or not resource_id:
            print(f"  ✗ Resource missing required fields (resourceType or id)")
            return False
        
        try:
            # Use PUT request for update/create
            url = f"{self.base_url}/{resource_type}/{resource_id}"
            response = self.session.put(url, json=resource, timeout=10)
            
            if response.status_code in [200, 201]:
                print(f"  ✓ {resource_type}/{resource_id} imported successfully")
                return True
            elif response.status_code in [400, 422]:
                # Parse error response
                try:
                    error_detail = response.json()
                    if 'issue' in error_detail:
                        for issue in error_detail['issue']:
                            diagnostics = issue.get('diagnostics', '')
                            severity = issue.get('severity', '')
                            print(f"  ✗ {resource_type}/{resource_id} validation failed [{severity}]: {diagnostics}")
                    else:
                        print(f"  ✗ {resource_type}/{resource_id} validation failed: {response.text[:300]}")
                except:
                    print(f"  ✗ {resource_type}/{resource_id} validation failed (HTTP {response.status_code}): {response.text[:300]}")
                return False
            else:
                print(f"  ✗ {resource_type}/{resource_id} import failed (HTTP {response.status_code}): {response.text[:200]}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"  ✗ {resource_type}/{resource_id} network error: {e}")
            return False
    
    def import_file(self, file_path: str) -> Dict[str, int]:
        """
        Import all resources from a single file
        
        Args:
            file_path: File path
            
        Returns:
            Dict: Import statistics {'success': n, 'failed': n}
        """
        result = {'success': 0, 'failed': 0}
        
        print(f"\n📄 Processing file: {os.path.basename(file_path)}")
        resources = self.load_mock_data(file_path)
        
        if not resources:
            print(f"  ⚠ No valid resources found in file")
            return result
        
        print(f"  Found {len(resources)} resources")
        
        for resource in resources:
            if self.import_resource(resource):
                result['success'] += 1
            else:
                result['failed'] += 1
        
        return result
    
    def import_all(self, mock_dir: str = "mock") -> None:
        """
        Batch import all mock data
        
        Args:
            mock_dir: Mock data directory
        """
        print("=" * 60)
        print("FHIR Resource Import Tool")
        print("=" * 60)
        
        # Check server connection
        if not self.check_server_connection():
            print("\nPlease ensure HAPI FHIR server is running (docker-compose up)")
            sys.exit(1)
        
        # Create dependency resources
        self.create_missing_resources()
        
        # Check mock directory
        if not os.path.exists(mock_dir):
            print(f"\n✗ Mock directory does not exist: {mock_dir}")
            sys.exit(1)
        
        print(f"\nStarting resource import...")
        
        # Import in dependency order
        for resource_type in self.import_order:
            file_path = os.path.join(mock_dir, f"{resource_type}.json")
            
            if not os.path.exists(file_path):
                print(f"\n⚠ File does not exist, skipping: {file_path}")
                continue
            
            result = self.import_file(file_path)
            self.stats['success'] += result['success']
            self.stats['failed'] += result['failed']
            self.stats['total'] += result['success'] + result['failed']
            
            # Create Patient-dependent resources after Patient import
            if resource_type == 'Patient' and result['success'] > 0:
                self.create_patient_dependent_resources()
        
        # Print statistics
        self.print_summary()
    
    def print_summary(self) -> None:
        """Print import statistics summary"""
        print("\n" + "=" * 60)
        print("Import Completed - Statistics")
        print("=" * 60)
        print(f"Total: {self.stats['total']} resources")
        print(f"Success: {self.stats['success']}")
        print(f"Failed: {self.stats['failed']}")
        print("=" * 60)
        
        if self.stats['failed'] > 0:
            print("\n⚠ Some resources failed to import, please check error messages")
            sys.exit(1)
        else:
            print("\n✓ All resources imported successfully!")


def main():
    """Main function"""
    # Get script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    mock_dir = os.path.join(script_dir, "mock")
    
    # Create importer and execute import
    importer = FHIRImporter(base_url="http://localhost:19090/fhir")
    importer.import_all(mock_dir=mock_dir)


if __name__ == "__main__":
    main()
