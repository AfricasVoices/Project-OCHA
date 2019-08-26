import argparse
from collections import OrderedDict

import altair
from core_data_modules.logging import Logger
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import IOUtils

from src.lib import PipelineConfiguration

Logger.set_project_name("OCHA")
log = Logger(__name__)

IMG_SCALE_FACTOR = 10  # Increase this to increase the resolution of the outputted PNGs

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates graphs for analysis")

    parser.add_argument("user", help="User launching this program")
    parser.add_argument("messages_json_input_path", metavar="messages-json-input-path",
                        help="Path to a JSONL file to read the TracedData of the messages data from")
    parser.add_argument("individuals_json_input_path", metavar="individuals-json-input-path",
                        help="Path to a JSONL file to read the TracedData of the messages data from")
    parser.add_argument("output_dir", metavar="output-dir",
                        help="Directory to write the output graphs to")

    args = parser.parse_args()

    user = args.user
    messages_json_input_path = args.messages_json_input_path
    individuals_json_input_path = args.individuals_json_input_path
    output_dir = args.output_dir

    IOUtils.ensure_dirs_exist(output_dir)

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
    for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
        for cc in plan.coding_configurations:
            if cc.analysis_file_key is None:
                continue

            log.info(f"Graphing the distribution of codes for {cc.analysis_file_key}...")
            label_counts = OrderedDict()
            for code in cc.code_scheme.codes:
                label_counts[code.string_value] = 0

            for ind in individuals:
                label_counts[ind[cc.analysis_file_key]] += 1

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
