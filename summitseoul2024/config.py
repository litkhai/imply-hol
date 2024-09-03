import base64
from multiprocessing import cpu_count

# Function to read the API token from a file
def read_api_token(file_path):
    with open(file_path, 'r') as file:
        return file.readline().strip()

# Read API token from the provided file
API_TOKEN = read_api_token('apikey.txt')

# Encode API_TOKEN for Basic Authentication
ENCODED_API_TOKEN = base64.b64encode(f'{API_TOKEN}:'.encode('utf-8')).decode('utf-8')

# API URL and headers configuration
ORGANIZATION_NAME = "조직명"
REGION = "ap-northeast-2"
CLOUD_PROVIDER = "aws"
PROJECT_ID = "프로젝트명"
CONNECTION_NAME = '커넥션이름'

INGESTION_URL = f"https://{ORGANIZATION_NAME}.{REGION}.{CLOUD_PROVIDER}.api.imply.io/v1/projects/{PROJECT_ID}/events/{CONNECTION_NAME}"
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f'Basic {ENCODED_API_TOKEN}'
}

# Rider generation configuration
NUM_RIDERS = 1000  # Adjust as needed

# CPU allocation configuration
CPU_COUNT = cpu_count() // 2  # Use 50% of the CPU cores, adjustable

