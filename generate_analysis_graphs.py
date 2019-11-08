import argparse
import csv
import glob
import json
from collections import OrderedDict

import altair
from core_data_modules.cleaners import Codes
from core_data_modules.data_models.scheme import CodeTypes
from core_data_modules.logging import Logger
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import IOUtils
from storage.google_cloud import google_cloud_utils
from storage.google_drive import drive_client_wrapper

from src.lib import PipelineConfiguration
from src.lib.pipeline_configuration import CodingModes

Logger.set_project_name("OCHA")
log = Logger(__name__)

IMG_SCALE_FACTOR = 10  # Increase this to increase the resolution of the outputted PNGs

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates graphs for analysis")

    parser.add_argument("user", help="User launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                       help="Path to the pipeline configuration json file")

    parser.add_argument("messages_json_input_path", metavar="messages-json-input-path",
                        help="Path to a JSONL file to read the TracedData of the messages data from")
    parser.add_argument("individuals_json_input_path", metavar="individuals-json-input-path",
                        help="Path to a JSONL file to read the TracedData of the messages data from")
    parser.add_argument("output_dir", metavar="output-dir",
                        help="Directory to write the output graphs to")

    args = parser.parse_args()

    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_configuration_file_path = args.pipeline_configuration_file_path

    messages_json_input_path = args.messages_json_input_path
    individuals_json_input_path = args.individuals_json_input_path
    output_dir = args.output_dir

    IOUtils.ensure_dirs_exist(output_dir)

    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)

    if pipeline_configuration.drive_upload is not None:
        log.info(f"Downloading Google Drive service account credentials...")
        credentials_info = json.loads(google_cloud_utils.download_blob_to_string(
            google_cloud_credentials_file_path, pipeline_configuration.drive_upload.drive_credentials_file_url))
        drive_client_wrapper.init_client_from_info(credentials_info)

    # Read the messages dataset
    log.info(f"Loading the messages dataset from {messages_json_input_path}...")
    with open(messages_json_input_path) as f:
        messages = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
    log.info(f"Loaded {len(messages)} messages")

    # Read the individuals dataset
    log.info(f"Loading the individuals dataset from {individuals_json_input_path}...")
    with open(individuals_json_input_path) as f:
        individuals = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
    log.info(f"Loaded {len(individuals)} individuals")

    # Compute the number of messages, individuals, and relevant messages per episode and overall.
    log.info("Computing the per-episode and per-season engagement counts...")
    engagement_counts = OrderedDict()
    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        # TODO: Add another field to CodingPlan so that we can give the weeks better names than the raw_field
        engagement_counts[plan.raw_field] = {
            "Episode": plan.raw_field,
            "Total Messages": 0,
            "Relevant Messages": 0,
            "Total Participants": 0,
            "% Relevant Messages": None
        }
    engagement_counts["Total"] = {
        "Episode": "Total",
        "Total Messages": 0,
        "Relevant Messages": 0,
        "Total Participants": 0,
        "% Relevant Messages": None
    }

    # Compute, per episode and across the season:
    #  - Total Messages, by counting the number of consenting message objects that contain the raw_field key each week.
    #  - Relevant Messages, by counting the number of consenting message objects which are coded with codes of type
    #    CodeTypes.NORMAL. If a message was coded under multiple schemes, an additional assert is performed to ensure
    #    the message was labelled with the same code type across all of those schemes.
    for msg in messages:
        if msg["consent_withdrawn"] == Codes.FALSE:
            for plan in PipelineConfiguration.RQA_CODING_PLANS:
                if plan.raw_field in msg:
                    engagement_counts[plan.raw_field]["Total Messages"] += 1
                    engagement_counts["Total"]["Total Messages"] += 1

                    # Check all the code schemes for this variable contain the same code type
                    codes = []
                    for cc in plan.coding_configurations:
                        if cc.coding_mode == CodingModes.SINGLE:
                            codes.append(cc.code_scheme.get_code_with_id(msg[cc.coded_field]["CodeID"]))
                        else:
                            assert cc.coding_mode == CodingModes.MULTIPLE
                            for label in msg[cc.coded_field]:
                                codes.append(cc.code_scheme.get_code_with_id(label["CodeID"]))

                    assert len(codes) > 0
                    code_type = codes[0].code_type
                    for code in codes:
                        assert code.code_type == code_type

                    if code_type == CodeTypes.NORMAL:
                        engagement_counts[plan.raw_field]["Relevant Messages"] += 1
                        engagement_counts["Total"]["Relevant Messages"] += 1

    # Compute, per episode and across the season:
    #  - Total Participants, by counting the number of consenting individuals objects that contain the raw_field key
    #    each week.
    for ind in individuals:
        if ind["consent_withdrawn"] == Codes.FALSE:
            engagement_counts["Total"]["Total Participants"] += 1
            for plan in PipelineConfiguration.RQA_CODING_PLANS:
                if plan.raw_field in ind:
                    engagement_counts[plan.raw_field]["Total Participants"] += 1

    # Compute:
    #  - % Relevant Messages, by computing Relevant Messages / Total Messages * 100, to 1 decimal place.
    for count in engagement_counts.values():
        count["% Relevant Messages"] = round(count["Relevant Messages"] / count["Total Messages"] * 100, 1)

    # Export the engagement counts to a csv.
    with open(f"{output_dir}/engagement_counts.csv", "w") as f:
        headers = ["Episode", "Total Messages", "Relevant Messages", "% Relevant Messages", "Total Participants"]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        for row in engagement_counts.values():
            writer.writerow(row)
            
    log.info("Graphing the per-episode engagement counts...")
    # Graph the number of messages in each show
    altair.Chart(
        altair.Data(values=[{"episode": x["Episode"], "count": x["Total Participants"]}
                            for x in engagement_counts.values() if x["Episode"] != "Total"])
    ).mark_bar().encode(
        x=altair.X("episode:N", title="Episode"),
        y=altair.Y("count:Q", title="Number of Individuals")
    ).properties(
        title="Individuals per episode"
    ).save(f"{output_dir}/individuals_per_episode.png", scale_factor=IMG_SCALE_FACTOR)

    # Graph the number of individuals in each episode
    altair.Chart(
        altair.Data(values=[{"episode": x["Episode"], "count": x["Total Participants"]}
                            for x in engagement_counts.values() if x["Episode"] != "Total"])
    ).mark_bar().encode(
        x=altair.X("episode:N", title="Episode"),
        y=altair.Y("count:Q", title="Number of Individuals")
    ).properties(
        title="Individuals per Show"
    ).save(f"{output_dir}/individuals_per_show.png", scale_factor=IMG_SCALE_FACTOR)

    # Plot the per-season distribution of responses for each survey question, per individual
    for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS:
        for cc in plan.coding_configurations:
            if cc.analysis_file_key is None:
                continue

            log.info(f"Graphing the distribution of codes for {cc.analysis_file_key}...")
            label_counts = OrderedDict()
            for code in cc.code_scheme.codes:
                label_counts[code.string_value] = 0

            if cc.coding_mode == CodingModes.SINGLE:
                for ind in individuals:
                    label_counts[ind[cc.analysis_file_key]] += 1
            else:
                assert cc.coding_mode == CodingModes.MULTIPLE
                for ind in individuals:
                    for code in cc.code_scheme.codes:
                        if ind[f"{cc.analysis_file_key}{code.string_value}"] == Codes.MATRIX_1:
                            label_counts[code.string_value] += 1

            chart = altair.Chart(
                altair.Data(values=[{"label": k, "count": v} for k, v in label_counts.items()])
            ).mark_bar().encode(
                x=altair.X("label:N", title="Label", sort=list(label_counts.keys())),
                y=altair.Y("count:Q", title="Number of Individuals")
            ).properties(
                title=f"Season Distribution: {cc.analysis_file_key}"
            )
            chart.save(f"{output_dir}/season_distribution_{cc.analysis_file_key}.html")
            chart.save(f"{output_dir}/season_distribution_{cc.analysis_file_key}.png", scale_factor=IMG_SCALE_FACTOR)

    if pipeline_configuration.drive_upload is not None:
        log.info("Uploading graphs to Drive...")
        paths_to_upload = glob.glob(f"{output_dir}/*.png")
        for i, path in enumerate(paths_to_upload):
            log.info(f"Uploading graph {i + 1}/{len(paths_to_upload)}: {path}...")
            drive_client_wrapper.update_or_create(path, pipeline_configuration.drive_upload.analysis_graphs_dir,
                                                  target_folder_is_shared_with_me=True)
    else:
        log.info("Skipping uploading to Google Drive (because the pipeline configuration json does not contain the key "
                 "'DriveUploadPaths')")

