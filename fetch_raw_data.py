import argparse
import json

from core_data_modules.cleaners import Codes, PhoneCleaner
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.logging import Logger
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import IOUtils, TimeUtils
from id_infrastructure.firestore_uuid_table import FirestoreUuidTable
from rapid_pro_tools.rapid_pro_client import RapidProClient
from storage.google_cloud import google_cloud_utils
from temba_client.v2 import Contact, Run

from src.lib import PipelineConfiguration, CodeSchemes
from src.lib.pipeline_configuration import RapidProSource, GCloudBucketSource

Logger.set_project_name("OCHA")
log = Logger(__name__)


def set_operator(user, traced_runs, phone_number_uuid_table):
    # Set the operator codes for each message.
    uuids = {td["avf_phone_id"] for td in traced_runs}
    uuid_to_phone_lut = phone_number_uuid_table.uuid_to_data_batch(uuids)
    for td in traced_runs:
        operator_code = PhoneCleaner.clean_operator(uuid_to_phone_lut[td["avf_phone_id"]])
        if operator_code == Codes.NOT_CODED:
            operator_label = CleaningUtils.make_label_from_cleaner_code(
                CodeSchemes.SOMALIA_OPERATOR,
                CodeSchemes.SOMALIA_OPERATOR.get_code_with_control_code(Codes.NOT_CODED),
                Metadata.get_call_location()
            )
        else:
            operator_label = CleaningUtils.make_label_from_cleaner_code(
                CodeSchemes.SOMALIA_OPERATOR,
                CodeSchemes.SOMALIA_OPERATOR.get_code_with_match_value(operator_code),
                Metadata.get_call_location()
            )
        td.append_data({"operator_coded": operator_label.to_dict()},
                       Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))


def fetch_from_rapid_pro(user, google_cloud_credentials_file_path, raw_data_dir, phone_number_uuid_table,
                         rapid_pro_test_contact_uuids, rapid_pro_source):
    log.info("Fetching data from Rapid Pro...")
    log.info("Downloading Rapid Pro access token...")
    rapid_pro_token = google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path, rapid_pro_source.token_file_url).strip()

    rapid_pro = RapidProClient(rapid_pro_source.domain, rapid_pro_token)

    # Load the previous export of contacts if it exists, otherwise fetch all contacts from Rapid Pro.
    raw_contacts_path = f"{raw_data_dir}/{rapid_pro_source.contacts_file_name}_raw.json"
    contacts_log_path = f"{raw_data_dir}/{rapid_pro_source.contacts_file_name}_log.jsonl"
    try:
        log.info(f"Loading raw contacts from file '{raw_contacts_path}'...")
        with open(raw_contacts_path) as raw_contacts_file:
            raw_contacts = [Contact.deserialize(contact_json) for contact_json in json.load(raw_contacts_file)]
        log.info(f"Loaded {len(raw_contacts)} contacts")
    except FileNotFoundError:
        log.info(f"File '{raw_contacts_path}' not found, will fetch all contacts from the Rapid Pro server")
        with open(contacts_log_path, "a") as contacts_log_file:
            raw_contacts = rapid_pro.get_raw_contacts(raw_export_log_file=contacts_log_file)

    # Download all the runs for each of the radio shows
    for flow in rapid_pro_source.activation_flow_names + rapid_pro_source.survey_flow_names:
        runs_log_path = f"{raw_data_dir}/{flow}_log.jsonl"
        raw_runs_path = f"{raw_data_dir}/{flow}_raw.json"
        traced_runs_output_path = f"{raw_data_dir}/{flow}.jsonl"
        log.info(f"Exporting flow '{flow}' to '{traced_runs_output_path}'...")

        flow_id = rapid_pro.get_flow_id(flow)

        # Load the previous export of runs for this flow, and update them with the newest runs.
        # If there is no previous export for this flow, fetch all the runs from Rapid Pro.
        with open(runs_log_path, "a") as raw_runs_log_file:
            try:
                log.info(f"Loading raw runs from file '{raw_runs_path}'...")
                with open(raw_runs_path) as raw_runs_file:
                    raw_runs = [Run.deserialize(run_json) for run_json in json.load(raw_runs_file)]
                log.info(f"Loaded {len(raw_runs)} runs")
                raw_runs = rapid_pro.update_raw_runs_with_latest_modified(
                    flow_id, raw_runs, raw_export_log_file=raw_runs_log_file)
            except FileNotFoundError:
                log.info(f"File '{raw_runs_path}' not found, will fetch all runs from the Rapid Pro server for flow '{flow}'")
                raw_runs = rapid_pro.get_raw_runs_for_flow_id(flow_id, raw_export_log_file=raw_runs_log_file)

        # Fetch the latest contacts from Rapid Pro.
        with open(contacts_log_path, "a") as raw_contacts_log_file:
            raw_contacts = rapid_pro.update_raw_contacts_with_latest_modified(raw_contacts,
                                                                              raw_export_log_file=raw_contacts_log_file)

        # Convert the runs to TracedData.
        traced_runs = rapid_pro.convert_runs_to_traced_data(
            user, raw_runs, raw_contacts, phone_number_uuid_table, rapid_pro_test_contact_uuids)

        log.info(f"Saving {len(raw_runs)} raw runs to {raw_runs_path}...")
        with open(raw_runs_path, "w") as raw_runs_file:
            json.dump([run.serialize() for run in raw_runs], raw_runs_file)
        log.info(f"Saved {len(raw_runs)} raw runs")

        log.info(f"Saving {len(traced_runs)} traced runs to {traced_runs_output_path}...")
        IOUtils.ensure_dirs_exist_for_file(traced_runs_output_path)
        with open(traced_runs_output_path, "w") as traced_runs_output_file:
            TracedDataJsonIO.export_traced_data_iterable_to_jsonl(traced_runs, traced_runs_output_file)
        log.info(f"Saved {len(traced_runs)} traced runs")

    log.info(f"Saving {len(raw_contacts)} raw contacts to file '{raw_contacts_path}'...")
    with open(raw_contacts_path, "w") as raw_contacts_file:
        json.dump([contact.serialize() for contact in raw_contacts], raw_contacts_file)
    log.info(f"Saved {len(raw_contacts)} contacts")
    
    
def fetch_from_gcloud_bucket(google_cloud_credentials_file_path, raw_data_dir, gcloud_source):
    log.info("Fetching data from a gcloud bucket...")
    for blob_url in gcloud_source.activation_flow_urls + gcloud_source.survey_flow_urls:
        flow = blob_url.split("/")[-1]

        traced_runs_output_path = f"{raw_data_dir}/{flow}"
        log.info(f"Saving {flow} to file '{traced_runs_output_path}'...")
        with open(traced_runs_output_path, "wb") as traced_runs_output_file:
            google_cloud_utils.download_blob_to_file(
                google_cloud_credentials_file_path, blob_url, traced_runs_output_file)


def main(user, google_cloud_credentials_file_path, pipeline_configuration_file_path, raw_data_dir):
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

    log.info(f"Fetching data from {len(pipeline_configuration.raw_data_sources)} sources...")
    for i, raw_data_source in enumerate(pipeline_configuration.raw_data_sources):
        log.info(f"Fetching from source {i + 1}/{len(pipeline_configuration.raw_data_sources)}...")
        if isinstance(raw_data_source, RapidProSource):
            fetch_from_rapid_pro(user, google_cloud_credentials_file_path, raw_data_dir, phone_number_uuid_table,
                                 pipeline_configuration.rapid_pro_test_contact_uuids, raw_data_source)
        elif isinstance(raw_data_source, GCloudBucketSource):
            fetch_from_gcloud_bucket(google_cloud_credentials_file_path, raw_data_dir, raw_data_source)
        else:
            assert False, f"Unknown raw_data_source type {type(raw_data_source)}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetches all the raw data for this project from Rapid Pro. "
                                                 "This script must be run from its parent directory.")

    parser.add_argument("user", help="Identifier of the user launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to the pipeline configuration json file"),
    parser.add_argument("raw_data_dir", metavar="raw-data-dir",
                        help="Path to a directory to save the raw data to")

    args = parser.parse_args()

    main(args.user, args.google_cloud_credentials_file_path, args.pipeline_configuration_file_path, args.raw_data_dir)
