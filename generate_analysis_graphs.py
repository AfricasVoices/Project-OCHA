import argparse
import csv
import glob
import json
from collections import OrderedDict

import altair
from core_data_modules.cleaners import Codes
from core_data_modules.logging import Logger
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import IOUtils
from storage.google_cloud import google_cloud_utils
from storage.google_drive import drive_client_wrapper

from src import AnalysisUtils
from src.lib import PipelineConfiguration
from src.lib.pipeline_configuration import CodingModes

Logger.set_project_name("OCHA")
log = Logger(__name__)

IMG_SCALE_FACTOR = 10  # Increase this to increase the resolution of the outputted PNGs
CONSENT_WITHDRAWN_KEY = "consent_withdrawn"

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
    engagement_counts = OrderedDict()  # of episode name to counts
    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        engagement_counts[plan.dataset_name] = {
            "Episode": plan.dataset_name,

            "Total Messages": "-",  # Can't report this for individual weeks because the data has been overwritten with "STOP"
            "Total Messages with Opt-Ins": len(AnalysisUtils.filter_opt_ins(messages, CONSENT_WITHDRAWN_KEY, [plan])),
            "Total Labelled Messages": len(AnalysisUtils.filter_labelled(messages, CONSENT_WITHDRAWN_KEY, [plan])),
            "Total Relevant Messages": len(AnalysisUtils.filter_relevant(messages, CONSENT_WITHDRAWN_KEY, [plan])),

            "Total Participants": "-",
            "Total Participants with Opt-Ins": len(AnalysisUtils.filter_opt_ins(individuals, CONSENT_WITHDRAWN_KEY, [plan])),
            "Total Relevant Participants": len(AnalysisUtils.filter_relevant(individuals, CONSENT_WITHDRAWN_KEY, [plan]))
        }
    engagement_counts["Total"] = {
        "Episode": "Total",

        "Total Messages": len(messages),
        "Total Messages with Opt-Ins": len(AnalysisUtils.filter_opt_ins(messages, CONSENT_WITHDRAWN_KEY, PipelineConfiguration.RQA_CODING_PLANS)),
        "Total Labelled Messages": len(AnalysisUtils.filter_labelled(messages, CONSENT_WITHDRAWN_KEY, PipelineConfiguration.RQA_CODING_PLANS)),
        "Total Relevant Messages": len(AnalysisUtils.filter_relevant(messages, CONSENT_WITHDRAWN_KEY, PipelineConfiguration.RQA_CODING_PLANS)),

        "Total Participants": len(individuals),
        "Total Participants with Opt-Ins": len(AnalysisUtils.filter_opt_ins(individuals, CONSENT_WITHDRAWN_KEY, PipelineConfiguration.RQA_CODING_PLANS)),
        "Total Relevant Participants": len(AnalysisUtils.filter_relevant(individuals, CONSENT_WITHDRAWN_KEY, PipelineConfiguration.RQA_CODING_PLANS))
    }

    with open(f"{output_dir}/engagement_counts.csv", "w") as f:
        headers = [
            "Episode",
            "Total Messages", "Total Messages with Opt-Ins", "Total Labelled Messages", "Total Relevant Messages",
            "Total Participants", "Total Participants with Opt-Ins", "Total Relevant Participants"
        ]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        for row in engagement_counts.values():
            writer.writerow(row)

    log.info("Computing the participation frequencies...")
    repeat_participations = OrderedDict()
    for i in range(1, len(PipelineConfiguration.RQA_CODING_PLANS) + 1):
        repeat_participations[i] = {
            "Episodes Participated In": i,
            "Number of Individuals": 0,
            "% of Individuals": None
        }

    # Compute the number of individuals who participated each possible number of times, from 1 to <number of RQAs>
    # An individual is considered to have participated if they sent a message and didn't opt-out, regardless of the
    # relevance of any of their messages.
    for ind in individuals:
        if AnalysisUtils.withdrew_consent(ind, CONSENT_WITHDRAWN_KEY):
            continue

        weeks_participated = 0
        for plan in PipelineConfiguration.RQA_CODING_PLANS:
            if AnalysisUtils.opt_in(ind, CONSENT_WITHDRAWN_KEY, plan):
                weeks_participated += 1
        assert weeks_participated != 0, f"Found individual '{ind['uid']}' with no participation in any week"
        repeat_participations[weeks_participated]["Number of Individuals"] += 1

    # Compute the percentage of individuals who participated each possible number of times.
    # Percentages are computed out of the total number of participants who opted-in.
    total_individuals = len(AnalysisUtils.filter_opt_ins(
        individuals, CONSENT_WITHDRAWN_KEY, PipelineConfiguration.RQA_CODING_PLANS))
    for rp in repeat_participations.values():
        rp["% of Individuals"] = round(rp["Number of Individuals"] / total_individuals * 100, 1)

    # Export the participation frequency data to a csv
    with open(f"{output_dir}/repeat_participations.csv", "w") as f:
        headers = ["Episodes Participated In", "Number of Individuals", "% of Individuals"]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        for row in repeat_participations.values():
            writer.writerow(row)

    log.info("Computing the demographic distributions...")
    # Compute the number of individuals with each demographic code.
    # Count excludes individuals who withdrew consent. STOP codes in each scheme are not exported, as it would look
    # like 0 individuals opted out otherwise, which could be confusing.
    # TODO: Report percentages?
    # TODO: Handle distributions for other variables too or just demographics?
    # TODO: Categorise age
    demographic_distributions = OrderedDict()  # of analysis_file_key -> code string_value -> number of individuals
    for plan in PipelineConfiguration.DEMOG_CODING_PLANS:
        for cc in plan.coding_configurations:
            if cc.analysis_file_key is None:
                continue

            demographic_distributions[cc.analysis_file_key] = OrderedDict()
            for code in cc.code_scheme.codes:
                if code.control_code == Codes.STOP:
                    continue
                demographic_distributions[cc.analysis_file_key][code.string_value] = 0

    for ind in individuals:
        if ind["consent_withdrawn"] == Codes.TRUE:
            continue

        for plan in PipelineConfiguration.DEMOG_CODING_PLANS:
            for cc in plan.coding_configurations:
                if cc.analysis_file_key is None:
                    continue

                code = cc.code_scheme.get_code_with_code_id(ind[cc.coded_field]["CodeID"])
                if code.control_code == Codes.STOP:
                    continue
                demographic_distributions[cc.analysis_file_key][code.string_value] += 1

    with open(f"{output_dir}/demographic_distributions.csv", "w") as f:
        headers = ["Demographic", "Code", "Number of Individuals"]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        last_demographic = None
        for demographic, counts in demographic_distributions.items():
            for code_string_value, number_of_individuals in counts.items():
                writer.writerow({
                    "Demographic": demographic if demographic != last_demographic else "",
                    "Code": code_string_value,
                    "Number of Individuals": number_of_individuals
                })
                last_demographic = demographic

    # Compute the theme distributions
    log.info("Computing the theme distributions...")

    def make_survey_counts_dict():
        survey_counts = OrderedDict()
        survey_counts["Total"] = 0
        for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
            for cc in plan.coding_configurations:
                if cc.analysis_file_key is None:
                    continue

                for code in cc.code_scheme.codes:
                    if code.control_code == Codes.STOP:
                        continue  # Ignore STOP codes because we already excluded everyone who opted out.
                    survey_counts[f"{cc.analysis_file_key}:{code.string_value}"] = 0

        return survey_counts

    def update_survey_counts(survey_counts, td):
        for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
            for cc in plan.coding_configurations:
                if cc.analysis_file_key is None:
                    continue

                if cc.coding_mode == CodingModes.SINGLE:
                    codes = [cc.code_scheme.get_code_with_code_id(td[cc.coded_field]["CodeID"])]
                else:
                    assert cc.coding_mode == CodingModes.MULTIPLE
                    codes = [cc.code_scheme.get_code_with_code_id(label["CodeID"]) for label in td[cc.coded_field]]

                for code in codes:
                    if code.control_code == Codes.STOP:
                        continue
                    survey_counts[f"{cc.analysis_file_key}:{code.string_value}"] += 1
                

    episodes = OrderedDict()
    for episode_plan in PipelineConfiguration.RQA_CODING_PLANS:
        # Prepare empty counts of the survey responses for each variable
        themes = OrderedDict()
        episodes[episode_plan.raw_field] = themes
        for cc in episode_plan.coding_configurations:
            # TODO: Add support for CodingModes.SINGLE if we need it e.g. for IMAQAL?
            assert cc.coding_mode == CodingModes.MULTIPLE, "RQAs with single coding modes not supported"
            themes["Total"] = make_survey_counts_dict()
            for code in cc.code_scheme.codes:
                if code.control_code == Codes.STOP:
                    continue
                themes[f"{cc.analysis_file_key}{code.string_value}"] = make_survey_counts_dict()

        # Fill in the counts by iterating over every individual
        for td in individuals:
            if td["consent_withdrawn"] == Codes.TRUE:
                continue

            for cc in episode_plan.coding_configurations:
                assert cc.coding_mode == CodingModes.MULTIPLE
                themes["Total"]["Total"] += 1
                update_survey_counts(themes["Total"], td)
                for label in td[cc.coded_field]:
                    code = cc.code_scheme.get_code_with_code_id(label["CodeID"])
                    if code.control_code == Codes.STOP:
                        continue
                    themes[f"{cc.analysis_file_key}{code.string_value}"]["Total"] += 1
                    update_survey_counts(themes[f"{cc.analysis_file_key}{code.string_value}"], td)

    with open(f"{output_dir}/theme_distributions.csv", "w") as f:
        f.write("CAUTION: The totals reported here show the number of times each theme was reported not the "
                "number of individuals or messages. Demographic totals apply to all codes (including NA NC etc.) \n")

        headers = ["Question", "Variable"] + list(make_survey_counts_dict().keys())
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        last_row_episode = None
        for episode, themes in episodes.items():
            for theme, survey_counts in themes.items():
                row = {
                    "Question": episode if episode != last_row_episode else "",
                    "Variable": theme,
                }
                row.update(survey_counts)
                writer.writerow(row)
                last_row_episode = episode

    log.info("Graphing the per-episode engagement counts...")
    # Graph the number of messages in each episode
    altair.Chart(
        altair.Data(values=[{"episode": x["Episode"], "count": x["Total Messages"]}
                            for x in engagement_counts.values() if x["Episode"] != "Total"])
    ).mark_bar().encode(
        x=altair.X("episode:N", title="Episode"),
        y=altair.Y("count:Q", title="Number of Messages")
    ).properties(
        title="Messages per Episode"
    ).save(f"{output_dir}/messages_per_episode.png", scale_factor=IMG_SCALE_FACTOR)

    # Graph the number of participants in each episode
    altair.Chart(
        altair.Data(values=[{"episode": x["Episode"], "count": x["Total Participants"]}
                            for x in engagement_counts.values() if x["Episode"] != "Total"])
    ).mark_bar().encode(
        x=altair.X("episode:N", title="Episode"),
        y=altair.Y("count:Q", title="Number of Participants")
    ).properties(
        title="Participants per Episode"
    ).save(f"{output_dir}/participants_per_episode.png", scale_factor=IMG_SCALE_FACTOR)

    log.info("Graphing the demographic distributions...")
    for demographic, counts in demographic_distributions.items():
        log.info(f"Graphing the distribution of codes for {demographic}...")
        altair.Chart(
            altair.Data(values=[{"code_string_value": code_string_value, "number_of_individuals": number_of_individuals}
                                for code_string_value, number_of_individuals in counts.items()])
        ).mark_bar().encode(
            x=altair.X("code_string_value:N", title="Code", sort=list(counts.keys())),
            y=altair.Y("number_of_individuals:Q", title="Number of Individuals")
        ).properties(
            title=f"Season Distribution: {demographic}"
        ).save(f"{output_dir}/season_distribution_{demographic}.png", scale_factor=IMG_SCALE_FACTOR)

    # Plot the per-season distribution of responses for each survey question, per individual
    for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS:
        for cc in plan.coding_configurations:
            if cc.analysis_file_key is None:
                continue

            # Don't generate graphs for the demographics, as they were already generated above.
            # TODO: Update the demographic_distributions to include the distributions for all variables?
            if cc.analysis_file_key in demographic_distributions:
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
