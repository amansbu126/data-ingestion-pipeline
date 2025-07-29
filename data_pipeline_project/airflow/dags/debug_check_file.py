from configparser import ConfigParser
import os
import sys

# Add project root to sys.path (adjust if needed)
sys.path.append("/home/aman_kumar/wns_projects/data_pipeline_project")

def get_config():
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../resources/config.ini'))
    print(f"🔍 Config path being used: {config_path}")
    config = ConfigParser()
    files_read = config.read(config_path)
    print(f"📄 Files read: {files_read}")
    if not files_read:
        raise FileNotFoundError(f"❌ Config file not found or unreadable at: {config_path}")
    return config

def get_paths():
    config = get_config()
    # Use absolute paths directly from config
    json_path = os.path.abspath(config["local"]["json_file_path"])
    archive_folder = os.path.abspath(config["local"]["archive_dir"])
    json_filename = os.path.basename(json_path)
    return config, json_filename, json_path, archive_folder

def check_file_exists():
    _, _, json_path, _ = get_paths()
    print(f"🔍 Looking for file: {json_path}")
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"❌ No JSON file found at: {json_path}")
    else:
        print("✅ File found.")

# Run this for testing
check_file_exists()
