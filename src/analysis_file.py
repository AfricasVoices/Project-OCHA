import sys
import time

from core_data_modules.cleaners import Codes
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCSVIO
from core_data_modules.traced_data.util import FoldTracedData
from core_data_modules.util import TimeUtils

from src.lib import PipelineConfiguration, ConsentUtils
from src.lib.pipeline_configuration import CodingModes, FoldingModes


class AnalysisFile(object):
    @staticmethod
    def generate(user, data, csv_by_message_output_path, csv_by_individual_output_path):
        # Serializer is currently overflowing
        # TODO: Investigate/address the cause of this.
        sys.setrecursionlimit(15000)

        consent_withdrawn_key = "consent_withdrawn"
        for td in data:
            td.append_data({consent_withdrawn_key: Codes.FALSE},
                           Metadata(user, Metadata.get_call_location(), time.time()))

        # Set the list of keys to be exported and how they are to be handled when folding
        export_keys = ["uid", consent_withdrawn_key]
        bool_keys = [consent_withdrawn_key]
        equal_keys = ["uid"]
        concat_keys = []
        matrix_keys = []
        binary_keys = []
        for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS:
            for cc in plan.coding_configurations:
                if cc.analysis_file_key is None:
                    continue

                if cc.coding_mode == CodingModes.SINGLE:
                    export_keys.append(cc.analysis_file_key)

                    if cc.folding_mode == FoldingModes.ASSERT_EQUAL:
                        equal_keys.append(cc.analysis_file_key)
                    elif cc.folding_mode == FoldingModes.YES_NO_AMB:
                        binary_keys.append(cc.analysis_file_key)
                    else:
                        assert False, f"Incompatible folding_mode {plan.folding_mode}"
                else:
                    assert cc.folding_mode == FoldingModes.MATRIX
                    for code in cc.code_scheme.codes:
                        export_keys.append(f"{cc.analysis_file_key}{code.string_value}")
                        matrix_keys.append(f"{cc.analysis_file_key}{code.string_value}")

            export_keys.append(plan.raw_field)
            if plan.raw_field_folding_mode == FoldingModes.CONCATENATE:
                concat_keys.append(plan.raw_field)
            elif plan.raw_field_folding_mode == FoldingModes.ASSERT_EQUAL:
                equal_keys.append(plan.raw_field)
            else:
                assert False, f"Incompatible raw_field_folding_mode {plan.raw_field_folding_mode}"

        # Convert codes to their string/matrix values
        for td in data:
            analysis_dict = dict()
            for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS:
                for cc in plan.coding_configurations:
                    if cc.analysis_file_key is None:
                        continue

                    if cc.coding_mode == CodingModes.SINGLE:
                        analysis_dict[cc.analysis_file_key] = \
                            cc.code_scheme.get_code_with_id(td[cc.coded_field]["CodeID"]).string_value
                    else:
                        assert cc.coding_mode == CodingModes.MULTIPLE
                        show_matrix_keys = []
                        for code in cc.code_scheme.codes:
                            show_matrix_keys.append(f"{cc.analysis_file_key}{code.string_value}")

                        for label in td.get(cc.coded_field, []):
                            code_string_value = cc.code_scheme.get_code_with_id(label['CodeID']).string_value
                            analysis_dict[f"{cc.analysis_file_key}{code_string_value}"] = Codes.MATRIX_1

                        for key in show_matrix_keys:
                            if key not in analysis_dict:
                                analysis_dict[key] = Codes.MATRIX_0
            td.append_data(analysis_dict,
                           Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

        # Set consent withdrawn based on presence of data coded as "stop"
        ConsentUtils.determine_consent_withdrawn(
            user, data, PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS,
            consent_withdrawn_key
        )

        # Fold data to have one respondent per row
        to_be_folded = []
        for td in data:
            to_be_folded.append(td.copy())

        folded_data = FoldTracedData.fold_iterable_of_traced_data(
            user, data, fold_id_fn=lambda td: td["uid"],
            equal_keys=equal_keys, concat_keys=concat_keys, matrix_keys=matrix_keys, bool_keys=bool_keys,
            binary_keys=binary_keys
        )

        # Fix-up _NA and _NC keys, which are currently being set incorrectly by
        # FoldTracedData.fold_iterable_of_traced_data when there are multiple radio shows
        # TODO: Update FoldTracedData to handle NA and NC correctly under multiple radio shows
        for td in folded_data:
            for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS:
                for cc in plan.coding_configurations:
                    if cc.analysis_file_key is None:
                        continue

                    if cc.coding_mode == CodingModes.MULTIPLE:
                        if td.get(plan.raw_field, "") != "":
                            td.append_data({f"{cc.analysis_file_key}{Codes.TRUE_MISSING}": Codes.MATRIX_0},
                                           Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

                        contains_non_nc_key = False
                        for key in matrix_keys:
                            if key.startswith(cc.analysis_file_key) and not key.endswith(Codes.NOT_CODED) \
                                    and td.get(key) == Codes.MATRIX_1:
                                contains_non_nc_key = True
                        if not contains_non_nc_key:
                            td.append_data({f"{cc.analysis_file_key}{Codes.NOT_CODED}": Codes.MATRIX_1},
                                           Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

        # Process consent
        ConsentUtils.set_stopped(user, data, consent_withdrawn_key, additional_keys=export_keys)
        ConsentUtils.set_stopped(user, folded_data, consent_withdrawn_key, additional_keys=export_keys)

        # Output to CSV with one message per row
        with open(csv_by_message_output_path, "w") as f:
            TracedDataCSVIO.export_traced_data_iterable_to_csv(data, f, headers=export_keys)

        with open(csv_by_individual_output_path, "w") as f:
            TracedDataCSVIO.export_traced_data_iterable_to_csv(folded_data, f, headers=export_keys)

        return data, folded_data
