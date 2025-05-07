"""
Initialization script for the La Défense mobility data lake
This script sets up the data lake structure and tests connectivity
"""
import os
import boto3
from botocore.client import Config
import json
import requests
from dotenv import load_dotenv
from datetime import datetime
from configuration import config


def check_environment():
    """Verify that the environment is correctly configured"""
    print("Checking environment...")

    # Check if MinIO is running
    try:
        response = requests.get(config.DATA_LAKE["endpoint_url"], timeout=5)
        print("✅ MinIO is running")
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to MinIO. Please start the MinIO server.")
        print(
            "   Docker command: docker run -d -p 9000:9000 -p 9001:9001 -v ~/data-lake-ladefense:/data minio/minio server /data --console-address \":9001\"")
        return False

    # Check Python dependencies
    required_packages = ["boto3", "pandas", "requests", "python-dotenv", "pyarrow", "fastparquet", "beautifulsoup4",
                         "schedule"]
    missing_packages = []

    # for package in required_packages:
    #     try:
    #         __import__(package)
    #     except ImportError:
    #         missing_packages.append(package)
    #
    # if missing_packages:
    #     print(f"❌ Missing Python packages: {', '.join(missing_packages)}")
    #     print(f"   Install them with: pip install {' '.join(missing_packages)}")
    #     return False
    # else:
    #     print("✅ All Python dependencies are installed")

    # Check API keys in .env or prompt user
    load_dotenv()
    missing_api_keys = []

    api_keys = {
        "VISUAL_CROSSING_API_KEY": "weather (Visual Crossing)",
        "TOMTOM_API_KEY": "traffic (TomTom)",
    }

    for key, description in api_keys.items():
        if not os.getenv(key):
            missing_api_keys.append(f"{key} for {description}")

    if missing_api_keys:
        print("⚠️ Missing API keys in .env file:")
        for missing_key in missing_api_keys:
            print(f"   - {missing_key}")

        create_env = input("Do you want to create a .env file now? (y/n): ")
        if create_env.lower() == 'y':
            with open("../.env", "w") as env_file:
                for key, description in api_keys.items():
                    value = input(f"Enter API key for {description} ({key}): ")
                    env_file.write(f"{key}={value}\n")
            print("✅ .env file created successfully")
            load_dotenv()  # Reload environment variables
        else:
            print("⚠️ Some features may not work without API keys.")
    else:
        print("✅ All API keys are configured")

    return True


def get_s3_client():
    """Create and return an S3 client connected to MinIO"""
    return boto3.client(
        's3',
        endpoint_url=config.DATA_LAKE["endpoint_url"],
        aws_access_key_id=config.DATA_LAKE["access_key"],
        aws_secret_access_key=config.DATA_LAKE["secret_key"],
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )


def create_data_lake_structure():
    """Create the complete structure of the data lake"""
    print("\nCreating data lake structure...")

    s3 = get_s3_client()
    bucket_name = config.DATA_LAKE["bucket_name"]

    # Check if bucket already exists
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' already exists")
    except:
        # Create bucket
        s3.create_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' created successfully")

    # Create folder structure based on configuration
    created_folders = 0
    for zone, subfolders in config.FOLDER_STRUCTURE.items():
        for subfolder in subfolders:
            folder_path = f"{zone}/{subfolder}/"
            try:
                s3.put_object(Bucket=bucket_name, Key=folder_path)
                created_folders += 1
                print(f"  ✓ Folder '{folder_path}' created")
            except Exception as e:
                print(f"  ✗ Error creating folder '{folder_path}': {str(e)}")

    print(f"✅ {created_folders} folders created in the data lake")


def test_connectivity():
    """Test basic connectivity to MinIO and write a test file"""
    print("\nTesting data lake connectivity...")

    s3 = get_s3_client()
    bucket_name = config.DATA_LAKE["bucket_name"]

    test_data = {
        "timestamp": datetime.now().isoformat(),
        "test": "Data lake connectivity test",
        "status": "success"
    }

    try:
        # Write test file
        s3.put_object(
            Bucket=bucket_name,
            Key="test/connectivity_test.json",
            Body=json.dumps(test_data)
        )

        # Read it back
        response = s3.get_object(Bucket=bucket_name, Key="test/connectivity_test.json")
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)

        if data.get("status") == "success":
            print("✅ Successfully wrote and read data from the data lake")
            return True
        else:
            print("❌ Data validation failed")
            return False
    except Exception as e:
        print(f"❌ Connectivity test failed: {str(e)}")
        return False


def main():
    print("=== Initializing La Défense Mobility Data Lake ===\n")

    # Check environment
    if not check_environment():
        print("\n❌ Environment is not properly configured. Please fix the issues above.")
        return

    # Create data lake structure
    create_data_lake_structure()

    # Test connectivity
    if test_connectivity():
        print("\n=== Data Lake initialized successfully! ===")
        print("\nTo start automated extractions, run:")
        print("  python run_extractions.py")
        print("\nTo access the MinIO console:")
        print("  http://localhost:9001 (login: minioadmin / password: minioadmin)")
    else:
        print("\n❌ Data Lake initialization failed. Please check the errors above.")


if __name__ == "__main__":
    main()