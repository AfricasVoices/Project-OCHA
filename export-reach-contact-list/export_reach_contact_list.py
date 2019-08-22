import argparse
import csv
import sys

from core_data_modules.cleaners import Codes
from core_data_modules.logging import Logger
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import PhoneNumberUuidTable

Logger.set_project_name("OCHA")
log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exports a list of phone numbers for the consenting participants "
                                                 "to REACH")

    parser.add_argument("traced_data_path", metavar="traced-data-path",
                        help="Path to the REACH traced data file to extract phone numbers from")
    parser.add_argument("phone_number_uuid_table_path", metavar="phone-number-uuid-table-path",
                        help="JSON file containing the phone number <-> UUID lookup table for the messages/surveys "
                             "datasets")
    parser.add_argument("output_path", metavar="output-path",
                        help="CSV file to write the REACH contacts to")

    args = parser.parse_args()

    traced_data_path = args.traced_data_path
    phone_number_uuid_table_path = args.phone_number_uuid_table_path
    output_path = args.output_path

    sys.setrecursionlimit(15000)

    # Load the phone number <-> uuid table
    log.info(f"Loading the phone number <-> uuid table from file '{phone_number_uuid_table_path}'...")
    with open(phone_number_uuid_table_path, "r") as f:
        phone_number_uuid_table = PhoneNumberUuidTable.load(f)
    log.info(f"Loaded {len(phone_number_uuid_table.numbers())} contacts")
    
    # Load the REACH traced data
    log.info(f"Loading REACH traced data from file '{traced_data_path}'...")
    with open(traced_data_path, "r") as f:
        data = TracedDataJsonIO.import_json_to_traced_data_iterable(f)
    log.info(f"Loaded {len(data)} traced data objects")

    # Search the TracedData for consenting contacts
    log.info("Searching for consenting uuids...")
    consenting_uuids = set()
    for td in data:
        if td["withdrawn_consent"] == Codes.TRUE:
            continue
        consenting_uuids.add(td["UID"])
    log.info(f"Found {len(consenting_uuids)} consenting uuids")

    # Convert the uuids to phone numbers
    log.info("Converting the uuids to phone numbers...")
    phone_numbers = [f"+{phone_number_uuid_table.get_phone(uuid)}" for uuid in consenting_uuids]

    log.warning(f"Exporting {len(phone_numbers)} phone numbers to {output_path}...")
    with open(output_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=["URN:Tel", "Name"], lineterminator="\n")
        writer.writeheader()

        for n in phone_numbers:
            writer.writerow({
                "URN:Tel": n
            })

        log.info(f"Wrote {len(phone_numbers)} contacts to {output_path}")
