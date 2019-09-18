import argparse
import csv
import json

from core_data_modules.cleaners import PhoneCleaner
from core_data_modules.logging import Logger
from id_infrastructure.firestore_uuid_table import FirestoreUuidTable
from storage.google_cloud import google_cloud_utils

from src.lib import PipelineConfiguration

log = Logger(__name__)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="De-identifies a CSV by converting the phone numbers in "
                                                 "the specified column to avf phone ids")

    parser.add_argument("csv_input_path", metavar="recovered-csv-input-url",
                        help="Path to a CSV file to de-identify a column of")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to the pipeline configuration json file")
    parser.add_argument("column_to_de_identify", metavar="column-to-de-identify",
                        help="Name of the column containing phone numbers to be de-identified")
    parser.add_argument("de_identified_csv_output_path", metavar="de-identified-csv-output-path",
                        help="Path to write the de-identified CSV to")

    args = parser.parse_args()

    csv_input_path = args.csv_input_path
    pipeline_configuration_file_path = args.pipeline_configuration_file_path
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    column_to_de_identify = args.column_to_de_identify
    de_identified_csv_output_path = args.de_identified_csv_output_path

    # Read the settings from the configuration file
    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)

    log.info("Downloading Firestore UUID Table credentials...")
    firestore_uuid_table_credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        pipeline_configuration.phone_number_uuid_table.firebase_credentials_file_url
    ))

    phone_number_uuid_table = FirestoreUuidTable(
        pipeline_configuration.phone_number_uuid_table.table_name,
        firestore_uuid_table_credentials,
        "avf-phone-uuid-"
    )
    log.info("Initialised the Firestore UUID table")

    log.info(f"Loading csv from '{csv_input_path}'...")
    with open(csv_input_path, "r", encoding='utf-8-sig') as f:
        raw_data = list(csv.DictReader(f))
    log.info(f"Loaded {len(raw_data)} rows")

    log.info(f"Normalising phone numbers in column '{column_to_de_identify}'...")
    for row in raw_data:
        row[column_to_de_identify] = PhoneCleaner.normalise_phone(row[column_to_de_identify])

    log.info(f"De-identifying column '{column_to_de_identify}'...")
    phone_numbers = [row[column_to_de_identify] for row in raw_data]

    phone_to_uuid_lut = phone_number_uuid_table.data_to_uuid_batch(phone_numbers)
    for row in raw_data:
        row[column_to_de_identify] = phone_to_uuid_lut[row[column_to_de_identify]]

    log.info(f"Exporting {len(raw_data)} de-identified rows to {de_identified_csv_output_path}...")
    with open(de_identified_csv_output_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=raw_data[0].keys())
        writer.writeheader()

        for row in raw_data:
            writer.writerow(row)
    log.info(f"Exported de-identified csv")
