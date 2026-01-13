# Carris GTFS Saver

A Python script that automatically downloads GTFS (General Transit Feed Specification) data from Carris (Lisbon's public transport operator) and uploads it to AWS S3 with intelligent change detection using hash comparison.

## Features

- 📥 Downloads GTFS data from Carris website
- 🔐 Computes SHA256 hash for change detection
- ☁️ Uploads to AWS S3 storage
- ⚡ Skips upload if file hasn't changed (hash comparison)
- 📝 Stores hash in S3 for future comparisons
- 🧹 Automatic cleanup of local files

## Prerequisites

- Python 3.7 or higher
- AWS account with S3 access
- AWS credentials configured

## Installation

### Option 1: Local Installation

1. Clone or download this repository

2. Install required dependencies:
```bash
pip install -r requirements.txt
```

### Option 2: Docker

1. Copy the example environment file:
```bash
cp .env.example .env
```

2. Edit `.env` and add your AWS credentials and bucket name

3. Build and run with Docker:
```bash
docker build -t carris-gtfs-saver .
docker run --env-file .env carris-gtfs-saver
```

Or use Docker Compose:
```bash
docker-compose up
```

## AWS Configuration

### Option 1: Environment Variables
```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="your-region"  # e.g., eu-west-1
export S3_BUCKET_NAME="your-bucket-name"
```

### Option 2: AWS Credentials File
Configure your credentials in `~/.aws/credentials`:
```ini
[default]
aws_access_key_id = your-access-key
aws_secret_access_key = your-secret-key
```

And set the region in `~/.aws/config`:
```ini
[default]
region = your-region
```

Then set only the bucket name:
```bash
export S3_BUCKET_NAME="your-bucket-name"
```

### Option 3: IAM Role (for EC2/ECS/Lambda)
If running on AWS infrastructure, use an IAM role with S3 permissions.

## Usage

Run the script:
```bash
python carris_gtfs_saver.py
```

Or make it executable and run directly:
```bash
chmod +x carris_gtfs_saver.py
./carris_gtfs_saver.py
```

### Automated Scheduling

#### Using cron (Linux/Mac)
```bash
# Edit crontab
crontab -e

# Run daily at 3 AM
0 3 * * * cd /path/to/carris-gtfs-saver && /usr/bin/python3 carris_gtfs_saver.py >> /var/log/carris-gtfs-saver.log 2>&1
```

#### Using AWS Lambda
Deploy as a Lambda function with EventBridge (CloudWatch Events) trigger for scheduled execution.

## How It Works

1. **Download**: Fetches the latest GTFS zip file from Carris
2. **Hash Calculation**: Computes SHA256 hash of the downloaded file
3. **Hash Comparison**: Retrieves stored hash from S3 (`gtfs/hash.txt`)
4. **Decision**: 
   - If hashes match: Skip upload (no changes)
   - If hashes differ: Proceed with upload
5. **Upload**: Uploads GTFS file to S3 (`gtfs/GTFS_Carris.zip`)
6. **Hash Storage**: Saves new hash to S3 for future comparisons
7. **Cleanup**: Removes local temporary file

## S3 Structure

```
your-bucket/
└── gtfs/
    ├── GTFS_Carris.zip    # Latest GTFS data
    └── hash.txt            # SHA256 hash of current file
```

## IAM Permissions

Required S3 permissions for your AWS user/role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket-name",
        "arn:aws:s3:::your-bucket-name/*"
      ]
    }
  ]
}
```

## Logging

The script outputs detailed logs including:
- Download progress
- Hash calculation results
- S3 operations status
- Error messages

## Error Handling

- Network errors during download
- S3 access issues
- Missing environment variables
- File I/O errors

All errors are logged and the script exits with appropriate status codes.

## License

MIT License - Feel free to use and modify as needed.

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## About Carris

Carris is the primary bus operator in Lisbon, Portugal. Their GTFS data is publicly available and updated periodically.

GTFS URL: https://www.carris.pt/gtfs/GTFS_Carris.zip
