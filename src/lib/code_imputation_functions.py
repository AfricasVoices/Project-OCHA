import time

from core_data_modules.cleaners import Codes
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.cleaners.location_tools import SomaliaLocations
from core_data_modules.data_models.code_scheme import CodeTypes
from core_data_modules.traced_data import Metadata

from src.lib.code_schemes import CodeSchemes


def make_location_code(scheme, clean_value):
    if clean_value == Codes.NOT_CODED:
        return scheme.get_code_with_control_code(Codes.NOT_CODED)
    else:
        return scheme.get_code_with_match_value(clean_value)


def impute_somalia_location_codes(user, data, location_configurations):
    for td in data:
        # Up to 1 location code should have been assigned in Coda. Search for that code,
        # ensuring that only 1 has been assigned or, if multiple have been assigned, that they are non-conflicting
        # control codes
        location_code = None

        for cc in location_configurations:
            coda_code = cc.code_scheme.get_code_with_code_id(td[cc.coded_field]["CodeID"])
            if location_code is not None:
                if not (
                        coda_code.code_id == location_code.code_id or coda_code.control_code == Codes.NOT_REVIEWED):
                    location_code = CodeSchemes.MOGADISHU_SUB_DISTRICT.get_code_with_control_code(Codes.CODING_ERROR)
            elif coda_code.control_code != Codes.NOT_REVIEWED:
                location_code = coda_code

        # If no code was found, then this location is still not reviewed.
        # Synthesise a NOT_REVIEWED code accordingly.
        if location_code is None:
            location_code = CodeSchemes.MOGADISHU_SUB_DISTRICT.get_code_with_control_code(Codes.NOT_REVIEWED)

        # If a control code was found, set all other location keys to that control code,
        # otherwise convert the provided location to the other locations in the hierarchy.
        if location_code.code_type == CodeTypes.CONTROL:
            for cc in location_configurations:
                td.append_data({
                    cc.coded_field: CleaningUtils.make_label_from_cleaner_code(
                        cc.code_scheme,
                        cc.code_scheme.get_code_with_control_code(location_code.control_code),
                        Metadata.get_call_location()
                    ).to_dict()
                }, Metadata(user, Metadata.get_call_location(), time.time()))
        elif location_code.code_type == CodeTypes.META:
            for cc in location_configurations:
                td.append_data({
                    cc.coded_field: CleaningUtils.make_label_from_cleaner_code(
                        cc.code_scheme,
                        cc.code_scheme.get_code_with_meta_code(location_code.meta_code),
                        Metadata.get_call_location()
                    ).to_dict()
                }, Metadata(user, Metadata.get_call_location(), time.time()))
        else:
            assert location_code.code_type == CodeTypes.NORMAL
            location = location_code.match_values[0]
            td.append_data({
                "mogadishu_sub_district_coded": CleaningUtils.make_label_from_cleaner_code(
                    CodeSchemes.MOGADISHU_SUB_DISTRICT,
                    make_location_code(CodeSchemes.MOGADISHU_SUB_DISTRICT,
                                       SomaliaLocations.mogadishu_sub_district_for_location_code(location)),
                    Metadata.get_call_location()).to_dict(),
                "district_coded": CleaningUtils.make_label_from_cleaner_code(
                    CodeSchemes.SOMALIA_DISTRICT,
                    make_location_code(CodeSchemes.SOMALIA_DISTRICT,
                                       SomaliaLocations.district_for_location_code(location)),
                    Metadata.get_call_location()).to_dict(),
                "region_coded": CleaningUtils.make_label_from_cleaner_code(
                    CodeSchemes.SOMALIA_REGION,
                    make_location_code(CodeSchemes.SOMALIA_REGION,
                                       SomaliaLocations.region_for_location_code(location)),
                    Metadata.get_call_location()).to_dict(),
                "state_coded": CleaningUtils.make_label_from_cleaner_code(
                    CodeSchemes.SOMALIA_STATE,
                    make_location_code(CodeSchemes.SOMALIA_STATE,
                                       SomaliaLocations.state_for_location_code(location)),
                    Metadata.get_call_location()).to_dict(),
                "zone_coded": CleaningUtils.make_label_from_cleaner_code(
                    CodeSchemes.SOMALIA_ZONE,
                    make_location_code(CodeSchemes.SOMALIA_ZONE,
                                       SomaliaLocations.zone_for_location_code(location)),
                    Metadata.get_call_location()).to_dict()
            }, Metadata(user, Metadata.get_call_location(), time.time()))

        # Impute zone from operator
        if "location_raw" not in td:
            operator_str = CodeSchemes.SOMALIA_OPERATOR.get_code_with_code_id(td["operator_coded"]["CodeID"]).string_value
            zone_str = SomaliaLocations.zone_for_operator_code(operator_str)

            td.append_data({
                "zone_coded": CleaningUtils.make_label_from_cleaner_code(
                    CodeSchemes.SOMALIA_ZONE,
                    make_location_code(CodeSchemes.SOMALIA_ZONE,
                                       SomaliaLocations.state_for_location_code(zone_str)),
                    Metadata.get_call_location()).to_dict()
            }, Metadata(user, Metadata.get_call_location(), time.time()))
