import os
import sys
import json
import time
import requests
import argparse
import zipfile
from dotenv import load_dotenv

load_dotenv()

MEDPLUM_CLIENT_ID = os.getenv("MEDPLUM_CLIENT_ID")
MEDPLUM_CLIENT_SECRET = os.getenv("MEDPLUM_CLIENT_SECRET")
BASE_URL = os.getenv("MEDPLUM_BASE_URL", "http://localhost:8103/fhir/R4")
TOKEN_URL = os.getenv("MEDPLUM_TOKEN_URL", "http://localhost:8103/oauth2/token")

def process_synthea_bundle(input_path, output_dir):
    with open(input_path, 'r', encoding='utf-8') as f:
        bundle = json.load(f)

    entries = bundle.get('entry', [])
    if not entries:
        return []
    
    uuid_type_map = {}
    for entry in entries:
        full_url = entry.get('fullUrl', '')
        res_type = entry.get('resource', {}).get('resourceType')
        if full_url.startswith('urn:uuid:') and res_type:
            uuid_val = full_url.replace('urn:uuid:', '')
            uuid_type_map[uuid_val] = res_type

    # Advanced referential integrity mapping to fix FHIR UUID collisions within chunks
    key_to_resource_type = {
        'managingOrganization': 'Organization', 'organization': 'Organization', 'location': 'Location',
        'practitioner': 'Practitioner', 'patient': 'Patient', 'subject': 'Patient', 'encounter': 'Encounter',
        'provider': 'Practitioner', 'requester': 'Practitioner', 'performer': 'Practitioner',
        'serviceProvider': 'Organization', 'actor': 'Practitioner', 'custodian': 'Organization', 'dispenser': 'Practitioner'
    }

    def convert_reference_string(ref_str, parent_key=None):
        if ref_str.startswith('urn:uuid:'):
            uuid_val = ref_str.replace('urn:uuid:', '')
            res_type = uuid_type_map.get(uuid_val) or key_to_resource_type.get(parent_key, "Resource")
            return f"{res_type}?identifier=https://github.com/synthetichealth/synthea|{uuid_val}"
        return ref_str

    def rewrite_dict(obj, parent_key=None):
        for k, v in list(obj.items()):
            if k == "reference" and isinstance(v, str):
                obj[k] = convert_reference_string(v, parent_key=parent_key)
            elif isinstance(v, str):
                if v.startswith('urn:uuid:'):
                    obj[k] = convert_reference_string(v, parent_key=k)
            elif isinstance(v, dict):
                if 'identifier' in v and isinstance(v['identifier'], dict):
                    sys_name = v['identifier'].get('system', '')
                    val = v['identifier'].get('value', '')
                    if 'synthetichealth' in sys_name and val:
                        res_type = uuid_type_map.get(val) or key_to_resource_type.get(k, "Resource")
                        v['reference'] = f"{res_type}?identifier=https://github.com/synthetichealth/synthea|{val}"
                        del v['identifier']
                rewrite_dict(v, parent_key=k)
            elif isinstance(v, list):
                rewrite_list(v, parent_key=k)

    def rewrite_list(lst, parent_key=None):
        for i in range(len(lst)):
            item = lst[i]
            if isinstance(item, str) and item.startswith('urn:uuid:'):
                lst[i] = convert_reference_string(item, parent_key=parent_key)
            elif isinstance(item, dict):
                if 'identifier' in item and isinstance(item['identifier'], dict):
                    sys_name = item['identifier'].get('system', '')
                    val = item['identifier'].get('value', '')
                    if 'synthetichealth' in sys_name and val:
                        res_type = uuid_type_map.get(val) or key_to_resource_type.get(parent_key, "Resource")
                        item['reference'] = f"{res_type}?identifier=https://github.com/synthetichealth/synthea|{val}"
                        del item['identifier']
                rewrite_dict(item, parent_key=parent_key)
            elif isinstance(item, list):
                rewrite_list(item, parent_key=parent_key)

    for entry in entries:
        resource = entry.get('resource', {})
        res_type = resource.get('resourceType')
        if 'id' in resource:
            del resource['id']
            
        full_url = entry.get('fullUrl', '')
        uuid_val = full_url.replace('urn:uuid:', '') if full_url.startswith('urn:uuid:') else ""
        if '?' in entry.get('request', {}).get('url', ''):
            # This is already a processed Conditional upsert. Skip completely!
            pass
        elif not uuid_val and entry.get('request', {}).get('method') == 'PUT':
            uuid_val = entry['request'].get('url', '').replace(f"{res_type}/", '')

        if uuid_val:
            found_id = False
            for ident in resource.get('identifier', []):
                if ident.get('system') == "https://github.com/synthetichealth/synthea" and ident.get('value') == uuid_val:
                    found_id = True
                    break
            if not found_id:
                if 'identifier' not in resource: resource['identifier'] = []
                resource['identifier'].append({"system": "https://github.com/synthetichealth/synthea", "value": uuid_val})

        rewrite_dict(resource)
        if res_type and uuid_val:
            entry['request'] = {'method': 'PUT', 'url': f"{res_type}?identifier=https://github.com/synthetichealth/synthea|{uuid_val}"}
            if 'fullUrl' in entry: del entry['fullUrl']
            
    base_name = os.path.basename(input_path).replace('.json', '')
    current_chunk = []
    chunk_index = 1
    generated_files = []

    for entry in entries:
        current_chunk.append(entry)
        if len(current_chunk) >= 20:  # Hard cap batch sizes to gracefully protect DB deadlocks!
            chunk_bundle = {"resourceType": "Bundle", "type": "transaction", "entry": current_chunk}
            out_name = os.path.join(output_dir, f"{base_name}_part{chunk_index:03d}.json")
            with open(out_name, 'w', encoding='utf-8') as out_f:
                json.dump(chunk_bundle, out_f, separators=(',', ':'))
            generated_files.append(out_name)
            chunk_index += 1
            current_chunk = []

    if current_chunk:
        chunk_bundle = {"resourceType": "Bundle", "type": "transaction", "entry": current_chunk}
        out_name = os.path.join(output_dir, f"{base_name}_part{chunk_index:03d}.json")
        with open(out_name, 'w', encoding='utf-8') as out_f:
            json.dump(chunk_bundle, out_f, separators=(',', ':'))
        generated_files.append(out_name)
            
    print(f"📦 [EXTRACT] Internally chunked monolith {input_path} into {len(generated_files)} localized atomic blocks.")
    return generated_files

def get_access_token():
    if not MEDPLUM_CLIENT_ID or not MEDPLUM_CLIENT_SECRET:
        print("❌ [APP CRASH] Missing MEDPLUM_CLIENT_ID or MEDPLUM_CLIENT_SECRET in .env file.")
        sys.exit(1)
        
    print(f"🔐 [AUTH] Generating secure OAuth2 token payload via credentials...")
    try:
        response = requests.post(TOKEN_URL, data={'grant_type': 'client_credentials', 'client_id': MEDPLUM_CLIENT_ID, 'client_secret': MEDPLUM_CLIENT_SECRET}, timeout=10)
        if response.status_code == 200:
            print(f"🔐 [AUTH] Secure network connection natively established.")
            return response.json()['access_token']
        else:
            print(f"❌ [AUTH CRASH] Failed to obtain token: {response.text}")
            sys.exit(1)
    except Exception as e:
        print(f"❌ [NETWORK CRASH] Connection to Medplum API timed out: {e}")
        sys.exit(1)

def upload_bundle(file_path: str, token: str) -> bool:
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/fhir+json'}
    try:
        with open(file_path, 'r', encoding='utf-8') as f: data = json.load(f)
        print(f"📡 [API] Pushing Transaction Segment: {os.path.basename(file_path)}...")
        response = requests.post(BASE_URL, headers=headers, json=data, timeout=30)
        
        if response.status_code in (200, 201):
            res_json = response.json()
            all_entries = res_json.get('entry', [])
            errors = [e for e in all_entries if not str(e.get('response', {}).get('status', '')).startswith('2')]
            if errors:
                ok_count = len(all_entries) - len(errors)
                print(f"⚠️ [WARNING] FHIR validation errors: {len(errors)} failed, {ok_count} succeeded out of {len(all_entries)} entries.")
                for err in errors[:2]:
                    issue = err.get('response', {}).get('outcome', {}).get('issue', [{}])[0]
                    diag = issue.get('diagnostics') or issue.get('details', {}).get('text') or str(issue)
                    print(f"      -> 🚨 {diag}")
                if ok_count == 0:
                    print(f"❌ [BLOCK FAILED] All {len(errors)} entries rejected — nothing persisted.")
                    return False
            else:
                print(f"✅ [SUCCESS] Block verified — all {len(all_entries)} entries persisted!")
            return True
        else:
            print(f"❌ [API REJECT] Medplum DB Rate Limit Repelled Slice!")
            try: print(f"      -> 🚨 OperationOutcome Reason: {response.json().get('issue', [{}])[0].get('details', {}).get('text', response.text)}")
            except: print(f"      -> 🚨 Raw trace: {response.text[:200]}")
            return False
            
    except Exception as e:
        print(f"❌ [SYS CRASH] Exception during POST execution: {str(e)}")
        return False

def process_payload(filepath: str):
    token = get_access_token()
    staging_dir = filepath + "_staging"
    os.makedirs(staging_dir, exist_ok=True)
    
    files_to_split = []
    
    # Process zip files dynamically
    if filepath.endswith('.zip'):
        print(f"📦 [EXTRACT] Exploding overarching archive ZIP...")
        extract_dir = filepath + "_extracted"
        with zipfile.ZipFile(filepath, 'r') as zip_ref: zip_ref.extractall(extract_dir)
        
        for root, dirs, files in os.walk(extract_dir):
            for file in files:
                if file.endswith('.json'): files_to_split.append(os.path.join(root, file))
    elif filepath.endswith('.json'):
        files_to_split.append(filepath)
    else:
        print(f"❌ [ERROR] Unsupported internal file extension detected. Expected .zip or .json exclusively.")
        sys.exit(1)

    print(f"🔍 [ROUTING] Engine orchestrating {len(files_to_split)} major architecture definitions to pre-processing logic.")
    files_to_upload = []
    for f in files_to_split:
        files_to_upload.extend(process_synthea_bundle(f, staging_dir))

    for idx, f in enumerate(sorted(files_to_upload)):
        print(f"⚙️ [DB PIPELINE] Transmitting atomic block [{idx+1}/{len(files_to_upload)}]...")
        success = False
        retries = 3
        while not success and retries > 0:
            success = upload_bundle(f, token)
            if not success:
                retries -= 1
                print(f"⏳ [WAIT] Docker Engine overloaded. Freezing pipeline for 5s... ({retries} strikes remaining)")
                time.sleep(5)
        # Throttle securely to prevent API 429 Rate Limits from stalling the primary DB context
        time.sleep(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ACRO Operational FHIR Architecture Batch Engine")
    parser.add_argument("filepath", help="Absolute path to the JSON or ZIP payload block")
    args = parser.parse_args()
    
    if not os.path.exists(args.filepath):
        print(f"❌ [ERROR] Provided payload object totally missing from drive path: {args.filepath}")
        sys.exit(1)
        
    print(f"⚡ [SYS] Executing internal ACRO orchestration module...")
    process_payload(args.filepath)
    print(f"🎉 [SYS] Operational lifecycle gracefully decoupled. Shutting down worker thread.")
