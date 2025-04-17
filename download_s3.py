import boto3
import os

# 1. AWS 자격 증명 직접 입력
aws_access_key_id = "AKIARSJVEUEYSJWQNXUI"
aws_secret_access_key = "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"
region_name = "ap-northeast-2"  # 예: 서울 리전

# 2. 설정
bucket_name = "direp"
prefix = "images/cats/"  # S3 상의 폴더 경로
local_dir = "./downloaded_images"

# 3. S3 클라이언트 생성
s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)

# 4. 로컬 저장 경로 생성
os.makedirs(local_dir, exist_ok=True)

# 5. 폴더 내부 파일 목록 가져오기
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

# 6. 이미지 다운로드
if "Contents" in response:
    for obj in response["Contents"]:
        key = obj["Key"]
        if key.endswith((".jpg", ".jpeg", ".png", ".gif", ".webp")):
            filename = key.split("/")[-1]
            local_path = os.path.join(local_dir, filename)
            print(f"📥 Downloading {key} → {local_path}")
            s3.download_file(bucket_name, key, local_path)
else:
    print("❗️지정된 폴더에 파일이 없습니다.")
