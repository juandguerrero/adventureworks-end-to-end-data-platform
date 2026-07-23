import os
import boto3

from config.config import *
from config.logger import logger

# ----------------------------------------------------
# Required files expected in the S3 bucket
# ----------------------------------------------------
required_files = [
    "raw/address/Address.csv",
    "raw/country_region/CountryRegion.csv",
    "raw/customer/Customer.csv",
    "raw/employee/Employee.csv",
    "raw/person/Person.csv",
    "raw/product/Product.csv",
    "raw/product_category/ProductCategory.csv",
    "raw/product_subcategory/ProductSubcategory.csv",
    "raw/sales_order_detail/SalesOrderDetail.csv",
    "raw/sales_order_header/SalesOrderHeader.csv",
    "raw/sales_person/SalesPerson.csv",
    "raw/sales_territory/SalesTerritory.csv",
    "raw/state_province/StateProvince.csv"
]

logger.info("=" * 60)
logger.info("Starting S3 Extraction Layer")

try:

    # ----------------------------------------------------
    # Connect to AWS S3
    # ----------------------------------------------------
    logger.info("Connecting to AWS S3...")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=REGION
    )

    s3.list_buckets()

    print("✅ Connected successfully!")
    logger.info("Connected to AWS successfully.")

    # ----------------------------------------------------
    # Read bucket contents
    # ----------------------------------------------------
    logger.info(f"Checking bucket: {BUCKET}")

    response = s3.list_objects_v2(Bucket=BUCKET)

    files = [
        obj
        for obj in response.get("Contents", [])
        if not obj["Key"].endswith("/")
    ]

    print("\nFiles found:\n")

    for file in files:
        print(file["Key"])
        logger.info(file["Key"])

    print(f"\nTotal files: {len(files)}")
    logger.info(f"Total files found: {len(files)}")

    # ----------------------------------------------------
    # Validate required files
    # ----------------------------------------------------
    print("\nValidating required files...\n")
    logger.info("Validating required files...")

    available = [f["Key"] for f in files]

    for file in required_files:

        if file in available:
            print(f"✅ {file} found")
            logger.info(f"{file} found")

        else:
            print(f"❌ {file} missing")
            logger.warning(f"{file} missing")

    # ----------------------------------------------------
    # Download validated files
    # ----------------------------------------------------
    logger.info("Downloading validated CSV files...")

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    download_folder = os.path.join(project_root, "data", "raw")

    for file in required_files:

        if file in available:

            # Remove "raw/" from the S3 key
            relative_path = file.replace("raw/", "")

            # Local destination
            local_file = os.path.join(download_folder, relative_path)

            # Create directories if needed
            os.makedirs(os.path.dirname(local_file), exist_ok=True)

            # Download file
            s3.download_file(
                BUCKET,
                file,
                local_file
            )

            print(f"⬇ Downloaded: {file}")
            logger.info(f"Downloaded {file} -> {local_file}")

    logger.info("All validated files downloaded successfully.")

    # ----------------------------------------------------
    # Display metadata
    # ----------------------------------------------------
    print("\nReading file metadata...\n")
    logger.info("Reading metadata...")

    for file in files:

        print(f"Filename      : {file['Key']}")
        print(f"Size (bytes)  : {file['Size']}")
        print(f"Last Modified : {file['LastModified']}")
        print(f"Storage Class : {file['StorageClass']}")
        print(f"ETag          : {file['ETag']}")
        print("-" * 50)

        logger.info(
            f"{file['Key']} | "
            f"Size={file['Size']} bytes | "
            f"LastModified={file['LastModified']} | "
            f"StorageClass={file['StorageClass']} | "
            f"ETag={file['ETag']}"
        )

    logger.info("Extraction Layer completed successfully.")
    print("\n✅ Extraction Layer completed successfully!")

except Exception as e:

    logger.exception("Extraction Layer failed.")
    print(f"\n❌ Error: {e}")
