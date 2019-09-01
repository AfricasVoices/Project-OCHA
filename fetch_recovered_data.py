import argparse
import csv
from datetime import datetime
from io import StringIO

import pytz
from core_data_modules.logging import Logger
from core_data_modules.traced_data import Metadata, TracedData
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import TimeUtils, SHAUtils, IOUtils
from storage.google_cloud import google_cloud_utils

from src.lib import PipelineConfiguration

log = Logger(__name__)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Converts messages in de-identified recovery CSV format to TracedData "
                                                 "which can be processed by the pipeline")

    parser.add_argument("user", help="Identifier of the user launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to the pipeline configuration json file")
    parser.add_argument("raw_data_dir", metavar="raw-data-dir",
                        help="Path to a directory to save the raw data to")

    args = parser.parse_args()

    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_configuration_file_path = args.pipeline_configuration_file_path
    raw_data_dir = args.raw_data_dir

    # Load the pipeline configuration file
    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)

    for recovery_csv_url in pipeline_configuration.recovery_csv_urls:
        log.info(f"Downloading recovered data from '{recovery_csv_url}'...")
        raw_csv_string = StringIO(google_cloud_utils.download_blob_to_string(
            google_cloud_credentials_file_path, recovery_csv_url))
        raw_data = list(csv.DictReader(raw_csv_string))
        log.info(f"Downloaded {len(raw_data)} recovered messages")

        log.info("Converting the recovered messages to TracedData...")
        data = []
        for i, row in enumerate(raw_data):
            raw_date = row["Received On"]
            parsed_raw_date = datetime.strptime(raw_date, "%m/%d/%y %H:%M")
            localized_date = pytz.timezone("Africa/Mogadishu").localize(parsed_raw_date)

            assert row["Mobile No"].startswith("avf-phone-uuid-"), \
                f"The 'Mobile No' column for '{recovery_csv_url} contains an item that has not been de-identified " \
                f"into Africa's Voices Foundation's de-identification format. This may be done with de_identify_csv.py."

            d = {
                "avf_phone_id": row["Mobile No"],
                "message": row["Message Content"],
                "received_on": localized_date.isoformat(),
                "run_id": SHAUtils.sha_dict(row)
            }

            data.append(TracedData(d, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())))
        log.info("Converted the recovered messages to TracedData")

        traced_data_output_path = f"{raw_data_dir}/{recovery_csv_url.split('/')[-1].split('.')[0]}.json"
        log.info(f"Exporting {len(data)} TracedData items to {traced_data_output_path}...")
        IOUtils.ensure_dirs_exist_for_file(traced_data_output_path)
        with open(traced_data_output_path, "w") as f:
            TracedDataJsonIO.export_traced_data_iterable_to_jsonl(data, f)
        log.info(f"Exported TracedData")
