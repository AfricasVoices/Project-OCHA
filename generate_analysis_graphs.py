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

    # Compute the number of messages in each show and graph
    log.info(f"Graphing the number of messages received in response to each show...")
    messages_per_show = OrderedDict()  # Of radio show index to messages count
    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        messages_per_show[plan.raw_field] = 0

    for msg in messages:
        for plan in PipelineConfiguration.RQA_CODING_PLANS:
            if msg.get(plan.raw_field, "") != "" and msg["consent_withdrawn"] == "false":
                messages_per_show[plan.raw_field] += 1

    chart = altair.Chart(
        altair.Data(values=[{"show": k, "count": v} for k, v in messages_per_show.items()])
    ).mark_bar().encode(
        x=altair.X("show:N", title="Show", sort=list(messages_per_show.keys())),
        y=altair.Y("count:Q", title="Number of Messages")
    ).properties(
        title="Messages per Show"
    )
    chart.save(f"{output_dir}/messages_per_show.html")
    chart.save(f"{output_dir}/messages_per_show.png", scale_factor=IMG_SCALE_FACTOR)

    # Compute the number of messages, individuals, and relevant messages per week
    log.info("Computing the weekly counts...")
    weekly_counts = OrderedDict()
    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        # TODO: Add another field to CodingPlan so that we can give the weeks better names than the raw_field
        weekly_counts[plan.raw_field] = {
            "Episode": plan.raw_field,
            "Total Messages": 0,
            "Relevant Messages": 0,
            "Total Participants": 0,
            "% Relevant Messages": None
        }
    weekly_counts["Total"] = {
        "Episode": "Total",
        "Total Messages": 0,
        "Relevant Messages": 0,
        "Total Participants": 0,
        "% Relevant Messages": None
    }

    # TODO: Re-check all of these description messages
    # Compute, per week and across the season:
    #  - Total Messages, by counting the number of message objects that contain the raw_field key each week.
    #  - Relevant Messages, by counting the number of message objects which are coded with codes of type
    #    CodeTypes.NORMAL. If a message was coded under multiple schemes, an additional assert is performed to ensure
    #    the message was labelled with the same code type across all of those schemes.
    for msg in messages:
        for plan in PipelineConfiguration.RQA_CODING_PLANS:
            if msg["consent_withdrawn"] == Codes.FALSE:
                if plan.raw_field in msg and msg[plan.raw_field] != "":
                    weekly_counts[plan.raw_field]["Total Messages"] += 1
                    weekly_counts["Total"]["Total Messages"] += 1

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
                        weekly_counts[plan.raw_field]["Relevant Messages"] += 1
                        weekly_counts["Total"]["Relevant Messages"] += 1

    # Compute, per week and across the season:
    #  - Total Participants, by counting the number of individuals objects that contain the raw_field key each week.
    for ind in individuals:
        if ind["consent_withdrawn"] == Codes.FALSE:
            weekly_counts["Total"]["Total Participants"] += 1
            for plan in PipelineConfiguration.RQA_CODING_PLANS:
                if ind.get(plan.raw_field, "") != "":
                    weekly_counts[plan.raw_field]["Total Participants"] += 1

    # Compute:
    #  - % Relevant Messages, by computing Relevant Messages / Total Messages * 100, to 1 decimal place.
    for count in weekly_counts.values():
        count["% Relevant Messages"] = round(count["Relevant Messages"] / count["Total Messages"] * 100, 1)

    # Export the weekly counts to a csv.
    with open(f"{output_dir}/weekly_counts.csv", "w") as f:
        writer = csv.DictWriter(f, fieldnames=["Episode", "Total Messages", "Relevant Messages", "% Relevant Messages",
                                               "Total Participants"],
                                lineterminator="\n")
        writer.writeheader()
        for row in weekly_counts.values():
            writer.writerow(row)
            
    # TODO: Update the graph generation code to use the weekly_counts dict rather than performing additional local
    #       derivations of the participation figures
    exit(0)

    # Compute the number of individuals in each show and graph
    log.info(f"Graphing the number of individuals who responded to each show...")
    individuals_per_show = OrderedDict()  # Of radio show index to individuals count
    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        individuals_per_show[plan.raw_field] = 0

    for ind in individuals:
        for plan in PipelineConfiguration.RQA_CODING_PLANS:
            if ind.get(plan.raw_field, "") != "" and ind["consent_withdrawn"] == "false":
                individuals_per_show[plan.raw_field] += 1

    chart = altair.Chart(
        altair.Data(values=[{"show": k, "count": v} for k, v in individuals_per_show.items()])
    ).mark_bar().encode(
        x=altair.X("show:N", title="Show", sort=list(individuals_per_show.keys())),
        y=altair.Y("count:Q", title="Number of Individuals")
    ).properties(
        title="Individuals per Show"
    )
    chart.save(f"{output_dir}/individuals_per_show.html")
    chart.save(f"{output_dir}/individuals_per_show.png", scale_factor=IMG_SCALE_FACTOR)

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

