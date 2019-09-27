import json
from abc import ABC, abstractmethod
from urllib.parse import urlparse

from core_data_modules.cleaners import Codes, swahili, somali
from core_data_modules.data_models import validators
from dateutil.parser import isoparse

from src.lib import CodeSchemes, code_imputation_functions


class CodingModes(object):
    SINGLE = "SINGLE"
    MULTIPLE = "MULTIPLE"


class FoldingModes(object):
    ASSERT_EQUAL = "ASSERT_EQUAL"
    YES_NO_AMB = "YES_NO_AMB"
    CONCATENATE = "CONCATENATE"
    MATRIX = "MATRIX"


class CodingConfiguration(object):
    def __init__(self, coding_mode, code_scheme, coded_field, folding_mode, analysis_file_key=None, cleaner=None):
        assert coding_mode in {CodingModes.SINGLE, CodingModes.MULTIPLE}

        self.coding_mode = coding_mode
        self.code_scheme = code_scheme
        self.coded_field = coded_field
        self.analysis_file_key = analysis_file_key
        self.folding_mode = folding_mode
        self.cleaner = cleaner


# TODO: Rename CodingPlan to something like DatasetConfiguration?
class CodingPlan(object):
    def __init__(self, raw_field, coding_configurations, raw_field_folding_mode, coda_filename=None, ws_code=None,
                 time_field=None, run_id_field=None, icr_filename=None, id_field=None, code_imputation_function=None):
        self.raw_field = raw_field
        self.time_field = time_field
        self.run_id_field = run_id_field
        self.coda_filename = coda_filename
        self.icr_filename = icr_filename
        self.coding_configurations = coding_configurations
        self.code_imputation_function = code_imputation_function
        self.ws_code = ws_code
        self.raw_field_folding_mode = raw_field_folding_mode

        if id_field is None:
            id_field = "{}_id".format(self.raw_field)
        self.id_field = id_field


class PipelineConfiguration(object):
    RQA_CODING_PLANS = [
        CodingPlan(raw_field="rqa_s04e01_raw",
                   time_field="sent_on",
                   run_id_field="rqa_s04e01_run_id",
                   coda_filename="s04e01.json",
                   icr_filename="s04e01.csv",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S04E01_REASONS,
                           coded_field="rqa_s04e01_coded",
                           analysis_file_key="rqa_s04e01_",
                           folding_mode=FoldingModes.MATRIX
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s04e01"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),

        CodingPlan(raw_field="rqa_s04e02_raw",
                   time_field="sent_on",
                   run_id_field="rqa_s04e02_run_id",
                   coda_filename="s04e02.json",
                   icr_filename="s04e02.csv",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.S04E02_REASONS,
                           coded_field="rqa_s04e02_coded",
                           analysis_file_key="rqa_s04e02_",
                           folding_mode=FoldingModes.MATRIX
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("s04e02"),
                   raw_field_folding_mode=FoldingModes.CONCATENATE),
    ]

    @staticmethod
    def clean_age_with_range_filter(text):
        """
        Cleans age from the given `text`, setting to NC if the cleaned age is not in the range 10 <= age < 100.
        """
        age = swahili.DemographicCleaner.clean_age(text)
        if type(age) == int and 10 <= age < 100:
            return str(age)
            # TODO: Once the cleaners are updated to not return Codes.NOT_CODED, this should be updated to still return
            #       NC in the case where age is an int but is out of range
        else:
            return Codes.NOT_CODED

    @staticmethod
    def clean_district_if_no_mogadishu_sub_district(text):
        mogadishu_sub_district = somali.DemographicCleaner.clean_mogadishu_sub_district(text)
        if mogadishu_sub_district == Codes.NOT_CODED:
            return somali.DemographicCleaner.clean_somalia_district(text)
        else:
            return Codes.NOT_CODED

    SURVEY_CODING_PLANS = [
        CodingPlan(raw_field="operator_raw",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.SOMALIA_OPERATOR,
                           coded_field="operator_coded",
                           analysis_file_key="operator",
                           folding_mode=FoldingModes.ASSERT_EQUAL
                       )
                   ],
                   raw_field_folding_mode=FoldingModes.ASSERT_EQUAL),

        CodingPlan(raw_field="location_raw",
                   time_field="location_time",
                   coda_filename="location.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.MOGADISHU_SUB_DISTRICT,
                           cleaner=somali.DemographicCleaner.clean_mogadishu_sub_district,
                           coded_field="mogadishu_sub_district_coded",
                           # This code exists for compatibility with the previous CSAP demog datasets.
                           # Not including in the analysis file because this project is not in Mogadishu.
                           folding_mode=FoldingModes.ASSERT_EQUAL
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.SOMALIA_DISTRICT,
                           cleaner=lambda text: PipelineConfiguration.clean_district_if_no_mogadishu_sub_district(text),
                           coded_field="district_coded",
                           analysis_file_key="district",
                           folding_mode=FoldingModes.ASSERT_EQUAL
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.SOMALIA_REGION,
                           coded_field="region_coded",
                           analysis_file_key="region",
                           folding_mode=FoldingModes.ASSERT_EQUAL
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.SOMALIA_STATE,
                           coded_field="state_coded",
                           analysis_file_key="state",
                           folding_mode=FoldingModes.ASSERT_EQUAL
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.SOMALIA_ZONE,
                           coded_field="zone_coded",
                           analysis_file_key="zone",
                           folding_mode=FoldingModes.ASSERT_EQUAL
                       )
                   ],
                   code_imputation_function=code_imputation_functions.impute_somalia_location_codes,
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("location"),
                   raw_field_folding_mode=FoldingModes.ASSERT_EQUAL),

        CodingPlan(raw_field="gender_raw",
                   time_field="gender_time",
                   coda_filename="gender.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.GENDER,
                           cleaner=somali.DemographicCleaner.clean_gender,
                           coded_field="gender_coded",
                           analysis_file_key="gender",
                           folding_mode=FoldingModes.ASSERT_EQUAL
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("gender"),
                   raw_field_folding_mode=FoldingModes.ASSERT_EQUAL),

        CodingPlan(raw_field="age_raw",
                   time_field="age_time",
                   coda_filename="age.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.AGE,
                           cleaner=lambda text: PipelineConfiguration.clean_age_with_range_filter(text),
                           coded_field="age_coded",
                           analysis_file_key="age",
                           folding_mode=FoldingModes.ASSERT_EQUAL
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("age"),
                   raw_field_folding_mode=FoldingModes.ASSERT_EQUAL),

        CodingPlan(raw_field="recently_displaced_raw",
                   time_field="recently_displaced_time",
                   coda_filename="recently_displaced.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.RECENTLY_DISPLACED,
                           cleaner=somali.DemographicCleaner.clean_yes_no,
                           coded_field="recently_displaced_coded",
                           analysis_file_key="recently_displaced",
                           folding_mode=FoldingModes.ASSERT_EQUAL
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("recently displaced"),
                   raw_field_folding_mode=FoldingModes.ASSERT_EQUAL),

        CodingPlan(raw_field="in_idp_camp_raw",
                   time_field="in_idp_camp_time",
                   coda_filename="in_idp_camp.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.IN_IDP_CAMP,
                           cleaner=somali.DemographicCleaner.clean_yes_no,
                           coded_field="in_idp_camp_coded",
                           analysis_file_key="in_idp_camp",
                           folding_mode=FoldingModes.ASSERT_EQUAL
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("in idp camp"),
                   raw_field_folding_mode=FoldingModes.ASSERT_EQUAL),

        CodingPlan(raw_field="have_voice_raw",
                   time_field="have_voice_time",
                   coda_filename="have_voice.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.HAVE_VOICE_YES_NO_AMB,
                           cleaner=somali.DemographicCleaner.clean_yes_no,
                           coded_field="have_voice_yes_no_amb_coded",
                           analysis_file_key="have_voice_yes_no_amb",
                           folding_mode=FoldingModes.ASSERT_EQUAL
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("have_voice"),
                   raw_field_folding_mode=FoldingModes.ASSERT_EQUAL),

        CodingPlan(raw_field="suggestions_raw",
                   time_field="suggestions_time",
                   coda_filename="suggestions.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.MULTIPLE,
                           code_scheme=CodeSchemes.SUGGESTIONS,
                           coded_field="suggestions_coded",
                           analysis_file_key="suggestions_",
                           folding_mode=FoldingModes.MATRIX
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("suggestions"),
                   raw_field_folding_mode=FoldingModes.ASSERT_EQUAL)
    ]

    def __init__(self, raw_data_sources, phone_number_uuid_table,
                 rapid_pro_key_remappings, project_start_date, project_end_date, filter_test_messages, move_ws_messages,
                 flow_definitions_upload_url_prefix, memory_profile_upload_url_prefix, data_archive_upload_url_prefix,
                 drive_upload=None):
        """
        :param raw_data_sources: List of sources to pull the various raw run files from.
        :type raw_data_sources: list of RawDataSource
        :param phone_number_uuid_table: Configuration for the Firestore phone number <-> uuid table.
        :type phone_number_uuid_table: PhoneNumberUuidTable
        :param rapid_pro_key_remappings: List of rapid_pro_key -> pipeline_key remappings.
        :type rapid_pro_key_remappings: list of RapidProKeyRemapping
        :param project_start_date: When data collection started - all activation messages received before this date
                                   time will be dropped.
        :type project_start_date: datetime.datetime
        :param project_end_date: When data collection stopped - all activation messages received on or after this date
                                 time will be dropped.
        :type project_end_date: datetime.datetime
        :param filter_test_messages: Whether to filter out messages sent from the rapid_pro_test_contact_uuids
        :type filter_test_messages: bool
        :param move_ws_messages: Whether to move messages labelled as Wrong Scheme to the correct dataset.
        :type move_ws_messages: bool
        :param flow_definitions_upload_url_prefix: The prefix of the GS URL to upload serialised flow definitions to.
                                                   This prefix will be appended with the current datetime and the
                                                   ".json" file extension.
        :type flow_definitions_upload_url_prefix: str
        :param memory_profile_upload_url_prefix: The prefix of the GS URL to upload the memory profile log to.
                                                 This prefix will be appended by the id of the pipeline run (provided
                                                 as a command line argument), and the ".profile" file extension.
        :type memory_profile_upload_url_prefix: str
        :param drive_upload: Configuration for uploading to Google Drive, or None.
                             If None, does not upload to Google Drive.
        :type drive_upload: DriveUploadPaths | None
        """
        self.raw_data_sources = raw_data_sources
        self.phone_number_uuid_table = phone_number_uuid_table
        self.rapid_pro_key_remappings = rapid_pro_key_remappings
        self.project_start_date = project_start_date
        self.project_end_date = project_end_date
        self.filter_test_messages = filter_test_messages
        self.move_ws_messages = move_ws_messages
        self.drive_upload = drive_upload
        self.flow_definitions_upload_url_prefix = flow_definitions_upload_url_prefix
        self.memory_profile_upload_url_prefix = memory_profile_upload_url_prefix
        self.data_archive_upload_url_prefix = data_archive_upload_url_prefix

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        raw_data_sources = []
        for raw_data_source in configuration_dict["RawDataSources"]:
            if raw_data_source["SourceType"] == "RapidPro":
                raw_data_sources.append(RapidProSource.from_configuration_dict(raw_data_source))
            elif raw_data_source["SourceType"] == "GCloudBucket":
                raw_data_sources.append(GCloudBucketSource.from_configuration_dict(raw_data_source))
            elif raw_data_source["SourceType"] == "ShaqadoonCSV":
                raw_data_sources.append(ShaqadoonCSVSource.from_configuration_dict(raw_data_source))
            else:
                assert False, f"Unknown SourceType '{raw_data_source['SourceType']}'. " \
                              f"Must be 'RapidPro', 'GCloudBucket', or 'ShaqadoonCSV'."

        phone_number_uuid_table = PhoneNumberUuidTable.from_configuration_dict(
            configuration_dict["PhoneNumberUuidTable"])

        rapid_pro_key_remappings = []
        for remapping_dict in configuration_dict["RapidProKeyRemappings"]:
            rapid_pro_key_remappings.append(RapidProKeyRemapping.from_configuration_dict(remapping_dict))

        project_start_date = isoparse(configuration_dict["ProjectStartDate"])
        project_end_date = isoparse(configuration_dict["ProjectEndDate"])

        filter_test_messages = configuration_dict["FilterTestMessages"]
        move_ws_messages = configuration_dict["MoveWSMessages"]

        drive_upload_paths = None
        if "DriveUpload" in configuration_dict:
            drive_upload_paths = DriveUpload.from_configuration_dict(configuration_dict["DriveUpload"])

        flow_definitions_upload_url_prefix = configuration_dict["FlowDefinitionsUploadURLPrefix"]
        memory_profile_upload_url_prefix = configuration_dict["MemoryProfileUploadURLPrefix"]
        data_archive_upload_url_prefix = configuration_dict["DataArchiveUploadURLPrefix"]

        return cls(raw_data_sources, phone_number_uuid_table,
                   rapid_pro_key_remappings, project_start_date, project_end_date, filter_test_messages,
                   move_ws_messages,
                   flow_definitions_upload_url_prefix, memory_profile_upload_url_prefix, data_archive_upload_url_prefix,
                   drive_upload_paths)

    @classmethod
    def from_configuration_file(cls, f):
        return cls.from_configuration_dict(json.load(f))

    def validate(self):
        validators.validate_list(self.raw_data_sources, "raw_data_sources")
        for i, raw_data_source in enumerate(self.raw_data_sources):
            assert isinstance(raw_data_source, RawDataSource), f"raw_data_sources[{i}] is not of type of RawDataSource"
            raw_data_source.validate()

        assert isinstance(self.phone_number_uuid_table, PhoneNumberUuidTable)
        self.phone_number_uuid_table.validate()

        validators.validate_list(self.rapid_pro_key_remappings, "rapid_pro_key_remappings")
        for i, remapping in enumerate(self.rapid_pro_key_remappings):
            assert isinstance(remapping, RapidProKeyRemapping), \
                f"rapid_pro_key_mappings[{i}] is not of type RapidProKeyRemapping"
            remapping.validate()

        validators.validate_datetime(self.project_start_date, "project_start_date")
        validators.validate_datetime(self.project_end_date, "project_end_date")

        validators.validate_bool(self.filter_test_messages, "filter_test_messages")
        validators.validate_bool(self.move_ws_messages, "move_ws_messages")

        if self.drive_upload is not None:
            assert isinstance(self.drive_upload, DriveUpload), \
                "drive_upload is not of type DriveUpload"
            self.drive_upload.validate()

        validators.validate_string(self.flow_definitions_upload_url_prefix, "flow_definitions_upload_url_prefix")
        validators.validate_string(self.memory_profile_upload_url_prefix, "memory_profile_upload_url_prefix")


class RawDataSource(ABC):
    @abstractmethod
    def get_activation_flow_names(self):
        pass

    @abstractmethod
    def get_survey_flow_names(self):
        pass

    @abstractmethod
    def validate(self):
        pass


class RapidProSource(RawDataSource):
    def __init__(self, domain, token_file_url, contacts_file_name, activation_flow_names, survey_flow_names,
                 test_contact_uuids):
        """
        :param domain: URL of the Rapid Pro server to download data from.
        :type domain: str
        :param token_file_url: GS URL of a text file containing the authorisation token for the Rapid Pro server.
        :type token_file_url: str
        :param contacts_file_name:
        :type contacts_file_name: str
        :param activation_flow_names: The names of the RapidPro flows that contain the radio show responses.
        :type: activation_flow_names: list of str
        :param survey_flow_names: The names of the RapidPro flows that contain the survey responses.
        :type: survey_flow_names: list of str
        :param test_contact_uuids: Rapid Pro contact UUIDs of test contacts.
                                   Runs for any of those test contacts will be tagged with {'test_run': True},
                                   and dropped when the pipeline is run with "FilterTestMessages" set to true.
        :type test_contact_uuids: list of str
        """
        self.domain = domain
        self.token_file_url = token_file_url
        self.contacts_file_name = contacts_file_name
        self.activation_flow_names = activation_flow_names
        self.survey_flow_names = survey_flow_names
        self.test_contact_uuids = test_contact_uuids

        self.validate()

    def get_activation_flow_names(self):
        return self.activation_flow_names

    def get_survey_flow_names(self):
        return self.survey_flow_names

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        domain = configuration_dict["Domain"]
        token_file_url = configuration_dict["TokenFileURL"]
        contacts_file_name = configuration_dict["ContactsFileName"]
        activation_flow_names = configuration_dict.get("ActivationFlowNames", [])
        survey_flow_names = configuration_dict.get("SurveyFlowNames", [])
        test_contact_uuids = configuration_dict.get("TestContactUUIDs", [])

        return cls(domain, token_file_url, contacts_file_name, activation_flow_names,
                   survey_flow_names, test_contact_uuids)

    def validate(self):
        validators.validate_string(self.domain, "domain")
        validators.validate_string(self.token_file_url, "token_file_url")
        validators.validate_string(self.contacts_file_name, "contacts_file_name")

        validators.validate_list(self.activation_flow_names, "activation_flow_names")
        for i, activation_flow_name in enumerate(self.activation_flow_names):
            validators.validate_string(activation_flow_name, f"activation_flow_names[{i}]")

        validators.validate_list(self.survey_flow_names, "survey_flow_names")
        for i, survey_flow_name in enumerate(self.survey_flow_names):
            validators.validate_string(survey_flow_name, f"survey_flow_names[{i}]")

        validators.validate_list(self.test_contact_uuids, "test_contact_uuids")
        for i, contact_uuid in enumerate(self.test_contact_uuids):
            validators.validate_string(contact_uuid, f"test_contact_uuids[{i}]")


class AbstractRemoteURLSource(RawDataSource):
    def __init__(self, activation_flow_urls, survey_flow_urls):
        self.activation_flow_urls = activation_flow_urls
        self.survey_flow_urls = survey_flow_urls

        self.validate()

    def get_activation_flow_names(self):
        return [url.split('/')[-1].split('.')[0] for url in self.activation_flow_urls]

    def get_survey_flow_names(self):
        return [url.split('/')[-1].split('.')[0] for url in self.survey_flow_urls]

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        activation_flow_urls = configuration_dict.get("ActivationFlowURLs", [])
        survey_flow_urls = configuration_dict.get("SurveyFlowURLs", [])

        return cls(activation_flow_urls, survey_flow_urls)

    def validate(self):
        validators.validate_list(self.activation_flow_urls, "activation_flow_urls")
        for i, activation_flow_url in enumerate(self.activation_flow_urls):
            validators.validate_url(activation_flow_url, f"activation_flow_urls[{i}]", "gs")

        validators.validate_list(self.survey_flow_urls, "survey_flow_urls")
        for i, survey_flow_url in enumerate(self.survey_flow_urls):
            validators.validate_url(survey_flow_url, f"survey_flow_urls[{i}]", "gs")


class GCloudBucketSource(AbstractRemoteURLSource):
    def __init__(self, activation_flow_urls, survey_flow_urls):
        super().__init__(activation_flow_urls, survey_flow_urls)


class ShaqadoonCSVSource(AbstractRemoteURLSource):
    def __init__(self, activation_flow_urls, survey_flow_urls):
        super().__init__(activation_flow_urls, survey_flow_urls)


class PhoneNumberUuidTable(object):
    def __init__(self, firebase_credentials_file_url, table_name):
        """
        :param firebase_credentials_file_url: GS URL to the private credentials file for the Firebase account where
                                                 the phone number <-> uuid table is stored.
        :type firebase_credentials_file_url: str
        :param table_name: Name of the data <-> uuid table in Firebase to use.
        :type table_name: str
        """
        self.firebase_credentials_file_url = firebase_credentials_file_url
        self.table_name = table_name

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        firebase_credentials_file_url = configuration_dict["FirebaseCredentialsFileURL"]
        table_name = configuration_dict["TableName"]

        return cls(firebase_credentials_file_url, table_name)

    def validate(self):
        validators.validate_url(self.firebase_credentials_file_url, "firebase_credentials_file_url", scheme="gs")
        validators.validate_string(self.table_name, "table_name")


class RapidProKeyRemapping(object):
    def __init__(self, is_activation_message, rapid_pro_key, pipeline_key):
        """
        :param is_activation_message: Whether this re-mapping contains an activation message (activation messages need
                                   to be handled differently because they are not always in the correct flow)
        :type is_activation_message: bool
        :param rapid_pro_key: Name of key in the dataset exported via RapidProTools.
        :type rapid_pro_key: str
        :param pipeline_key: Name to use for that key in the rest of the pipeline.
        :type pipeline_key: str
        """
        self.is_activation_message = is_activation_message
        self.rapid_pro_key = rapid_pro_key
        self.pipeline_key = pipeline_key

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        is_activation_message = configuration_dict.get("IsActivationMessage", False)
        rapid_pro_key = configuration_dict["RapidProKey"]
        pipeline_key = configuration_dict["PipelineKey"]

        return cls(is_activation_message, rapid_pro_key, pipeline_key)

    def validate(self):
        validators.validate_bool(self.is_activation_message, "is_activation_message")
        validators.validate_string(self.rapid_pro_key, "rapid_pro_key")
        validators.validate_string(self.pipeline_key, "pipeline_key")


class DriveUpload(object):
    def __init__(self, drive_credentials_file_url, production_upload_path, messages_upload_path,
                 individuals_upload_path, messages_traced_data_upload_path, individuals_traced_data_upload_path,
                 analysis_graphs_dir):
        """
        :param drive_credentials_file_url: GS URL to the private credentials file for the Drive service account to use
                                           to upload the output files.
        :type drive_credentials_file_url: str
        :param production_upload_path: Path in the Drive service account's "Shared with Me" directory to upload the
                                       production CSV to.
        :type production_upload_path: str
        :param messages_upload_path: Path in the Drive service account's "Shared with Me" directory to upload the
                                     messages analysis CSV to.
        :type messages_upload_path: str
        :param individuals_upload_path: Path in the Drive service account's "Shared with Me" directory to upload the
                                        individuals analysis CSV to.
        :type individuals_upload_path: str
        :param messages_traced_data_upload_path: Path in the Drive service account's "Shared with Me" directory to 
                                                 upload the serialized messages TracedData from this pipeline run to.
        :type messages_traced_data_upload_path: str
        :param individuals_traced_data_upload_path: Path in the Drive service account's "Shared with Me" directory to
                                                    upload the serialized individuals TracedData from this pipeline
                                                    run to.
        :type individuals_traced_data_upload_path: str
        :param analysis_graphs_dir: Directory in the Drive service account's "Shared with Me" directory to upload the
                                    analysis graphs from this pipeline run to.
        :type analysis_graphs_dir: str
        """
        self.drive_credentials_file_url = drive_credentials_file_url
        self.production_upload_path = production_upload_path
        self.messages_upload_path = messages_upload_path
        self.individuals_upload_path = individuals_upload_path
        self.messages_traced_data_upload_path = messages_traced_data_upload_path
        self.individuals_traced_data_upload_path = individuals_traced_data_upload_path
        self.analysis_graphs_dir = analysis_graphs_dir

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        drive_credentials_file_url = configuration_dict["DriveCredentialsFileURL"]
        production_upload_path = configuration_dict["ProductionUploadPath"]
        messages_upload_path = configuration_dict["MessagesUploadPath"]
        individuals_upload_path = configuration_dict["IndividualsUploadPath"]
        messages_traced_data_upload_path = configuration_dict["MessagesTracedDataUploadPath"]
        individuals_traced_data_upload_path = configuration_dict["IndividualsTracedDataUploadPath"]
        analysis_graphs_dir = configuration_dict["AnalysisGraphsDir"]

        return cls(drive_credentials_file_url, production_upload_path, messages_upload_path,
                   individuals_upload_path, messages_traced_data_upload_path, individuals_traced_data_upload_path,
                   analysis_graphs_dir)

    def validate(self):
        validators.validate_string(self.drive_credentials_file_url, "drive_credentials_file_url")
        assert urlparse(self.drive_credentials_file_url).scheme == "gs", "DriveCredentialsFileURL needs to be a gs " \
                                                                         "URL (i.e. of the form gs://bucket-name/file)"

        validators.validate_string(self.production_upload_path, "production_upload_path")
        validators.validate_string(self.messages_upload_path, "messages_upload_path")
        validators.validate_string(self.individuals_upload_path, "individuals_upload_path")
        validators.validate_string(self.messages_traced_data_upload_path, "messages_traced_data_upload_path")
        validators.validate_string(self.individuals_traced_data_upload_path, "individuals_traced_data_upload_path")
        validators.validate_string(self.analysis_graphs_dir, "analysis_graphs_dir")
