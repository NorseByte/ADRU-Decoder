from pathlib import Path

import pandas as pd
import yaml

from adru_db_utils import initialize_adru_database, add_adru_file_to_db, add_message_file_to_db, \
    exist_txt_file_for_adru, fetch_newest_txt_file_for_adru, insert_messages_from_txt, \
    get_all_adru_files_that_has_txt_files_and_latest_txt, is_txt_content_in_db_with_entries, \
    enrich_dataframe_with_db_values
from adru_utils import find_adru_files, get_latest_txt_file, prompt_user_and_wait_for_txt, extract_unique_attributes

# Load YAML config
with open("config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

# Extract config values
exe_path = config["decode_tool"]["executable_path"]
adru_input_dir = config["input"]["adru_input_dir"]
input_type = config["input"]["input_type"]
output_types = config["input"]["output_types"]
txt_output_dir = config["output"]["txt_output_dir"]
csv_output_dir = config["output"]["csv_output_dir"]
csv_raw_dir = config["input"]["csv_input_dir"]

# Add new row in the database with file name and fetch id for it
# Example usage placeholder (adjust paths and attribute lists in real usage)
db_file = Path("adru-export.db")

jru_attributes = ['(1)', '(2)', '(3)', '(4)', '(5)', 'ADHESION', 'BALISE_ERROR_SECTION', 'BALISE_RECEPTION_ERROR',
                  'BALISE_SECTION', 'BRAKE_PERCENTAGE', 'BRAKE_PRESSURE_SECTION', 'BRAKING_CURVE_SUMMARY_SECTION',
                  'B_F', 'CURRENT_FULL_SERVICE_BRAKE_DEMANDS', 'DATA', 'DATE.DAY', 'DATE.MONTH', 'DATE.YEAR',
                  'DISTANT_SIGNALS_SECTION', 'DISTANT_SIGNAL_COUNT', 'DMI_SOUND_STATUS', 'DMI_SYMB_STATUS',
                  'DRIVER_ID', 'D_TARGET', 'EXTERNAL_BRAKE_PRESSURE', 'F1_ERROR_CHAR', 'F2_ERROR_CHAR',
                  'F3_ERROR_CHAR', 'INTERNAL_BRAKE_PRESSURE', 'JD_Message_ID', 'LAST_PASSED_SIGNAL_GROUP',
                  'LATCHED_FULL_SERVICE_BRAKE_DEMANDS', 'LINKING_ACTIVE', 'LINKING_POSITION', 'LINKING_SECTION',
                  'LOCATION', 'L_CAPTION[0]', 'L_CAPTION[1]', 'L_CAPTION[2]', 'L_CAPTION[3]', 'L_CAPTION[4]',
                  'L_CAPTION[5]', 'L_CAPTION[6]', 'L_CAPTION[7]', 'L_CAPTION[8]', 'L_MESSAGE', 'L_STMPACKET',
                  'L_TEXT', 'L_TRAIN', 'MAIN_INDICATOR_SO_CATEGORY', 'MAIN_INDICATOR_SO_INDEX', 'MAX_SPEEDS_SECTION',
                  'MVB_DEVICE_FAMILY', 'M_ACK', 'M_AIRTIGHT', 'M_AXLELOADCAT', 'M_BIEB_CMD', 'M_BISB_CMD',
                  'M_BRAKE_COMMAND_STATE', 'M_BRAKE_LAMBDA_CONF(0)', 'M_BRAKE_LAMBDA_CONF(1)',
                  'M_BRAKE_LAMBDA_CONF(2)', 'M_BRAKE_LAMBDA_CONF(3)', 'M_BRAKE_LAMBDA_CONF(4)',
                  'M_BRAKE_LAMBDA_CONF(5)', 'M_BRAKE_LAMBDA_CONF(6)', 'M_BRAKE_LAMBDA_CONF(7)', 'M_BRAKE_PERCENTAGE',
                  'M_BRAKE_POSITION', 'M_BUT_ATTRIB[0]', 'M_BUT_ATTRIB[1]', 'M_BUT_ATTRIB[2]', 'M_BUT_ATTRIB[3]',
                  'M_BUT_ATTRIB[4]', 'M_BUT_ATTRIB[5]', 'M_BUT_ATTRIB[6]', 'M_CAB_A_STATUS', 'M_CAB_B_STATUS',
                  'M_COLD_MVT', 'M_COLOUR_IS', 'M_COLOUR_PS', 'M_COLOUR_RS', 'M_COLOUR_SP', 'M_COLOUR_TS',
                  'M_DIRECTION_CONTROLLER', 'M_DISCREASON', 'M_DISCSENDER', 'M_DISCTYPE', 'M_DRIVERACTIONS', 'M_DUP',
                  'M_EDDYCURRENTBRAKE', 'M_ELECTROPNEUMATICBRAKE', 'M_EP_STATUS', 'M_IND_ATTRIB[0]',
                  'M_IND_ATTRIB[1]', 'M_IND_ATTRIB[2]', 'M_IND_ATTRIB[3]', 'M_IND_ATTRIB[4]', 'M_IND_ATTRIB[5]',
                  'M_IND_ATTRIB[6]', 'M_IND_ATTRIB[7]', 'M_IND_ATTRIB[8]', 'M_K1_EXCEEDING', 'M_LEVEL',
                  'M_LOADINGGAUGE', 'M_MAGNETICSHOEBRAKE', 'M_MCOUNT', 'M_MODE', 'M_NATIONAL_SYSTEM_ISOLATION',
                  'M_NOM_ROT_MASS', 'M_NON_LEADING', 'M_PASSIVE_SHUNTING', 'M_PT_CODE', 'M_REGENERATIVEBRAKE',
                  'M_SDMSUPSTAT', 'M_SDMTYPE', 'M_SLEEPING', 'M_TCO_COMMAND_STATE', 'M_TESTOK', 'M_TRACTION_STATUS',
                  'M_TRAIN_DATA_ENTRY', 'M_TTI', 'M_VERSION', 'M_XATTRIBUTE', 'NC_CDTRAIN', 'NC_TRAIN', 'NID_BG',
                  'NID_BUTPOS[0]', 'NID_BUTPOS[1]', 'NID_BUTPOS[2]', 'NID_BUTPOS[3]', 'NID_BUTPOS[4]',
                  'NID_BUTPOS[5]', 'NID_BUTPOS[6]', 'NID_BUTTON[0]', 'NID_BUTTON[1]', 'NID_BUTTON[2]',
                  'NID_BUTTON[3]', 'NID_BUTTON[4]', 'NID_BUTTON[5]', 'NID_BUTTON[6]', 'NID_C', 'NID_DMICHANNEL',
                  'NID_ENGINE', 'NID_ICON[0]', 'NID_ICON[1]', 'NID_ICON[2]', 'NID_ICON[3]', 'NID_ICON[4]',
                  'NID_ICON[5]', 'NID_ICON[6]', 'NID_ICON[7]', 'NID_ICON[8]', 'NID_INDICATOR[0]', 'NID_INDICATOR[1]',
                  'NID_INDICATOR[2]', 'NID_INDICATOR[3]', 'NID_INDICATOR[4]', 'NID_INDICATOR[5]', 'NID_INDICATOR[6]',
                  'NID_INDICATOR[7]', 'NID_INDICATOR[8]', 'NID_INDPOS[0]', 'NID_INDPOS[1]', 'NID_INDPOS[2]',
                  'NID_INDPOS[3]', 'NID_INDPOS[4]', 'NID_INDPOS[5]', 'NID_INDPOS[6]', 'NID_INDPOS[7]',
                  'NID_INDPOS[8]', 'NID_LRBG1', 'NID_LRBG2', 'NID_MESSAGE', 'NID_MESSAGE(SUBSET)', 'NID_NTC',
                  'NID_NTC(0)', 'NID_NTC(1)', 'NID_NTC(10)', 'NID_NTC(11)', 'NID_NTC(12)', 'NID_NTC(13)',
                  'NID_NTC(14)', 'NID_NTC(15)', 'NID_NTC(16)', 'NID_NTC(17)', 'NID_NTC(18)', 'NID_NTC(19)',
                  'NID_NTC(2)', 'NID_NTC(20)', 'NID_NTC(21)', 'NID_NTC(22)', 'NID_NTC(23)', 'NID_NTC(24)',
                  'NID_NTC(25)', 'NID_NTC(26)', 'NID_NTC(27)', 'NID_NTC(28)', 'NID_NTC(29)', 'NID_NTC(3)',
                  'NID_NTC(4)', 'NID_NTC(5)', 'NID_NTC(6)', 'NID_NTC(7)', 'NID_NTC(8)', 'NID_NTC(9)',
                  'NID_OPERATIONAL', 'NID_RADIO', 'NID_RBC', 'NID_SOUND[0]', 'NID_STMPACKET', 'NID_STMSTATE',
                  'NID_STMSTATEORDER', 'NID_STMX', 'NID_STM_EVENT', 'NID_TEST', 'NID_XMESSAGE', 'N_AXLE',
                  'N_BRAKE_CONF', 'N_ITER', 'N_PIG', 'N_TOTAL', 'N_VERMAJOR', 'N_VERMINOR', 'ODOMETER_ESTIMATED',
                  'ODOMETER_MAXIMUM', 'ODOMETER_MINIMUM', 'PERMISSION_TO_PASS_LANDSLIDE', 'PERMISSION_TO_PASS_STOP',
                  'PRE_INDICATOR_SO_CATEGORY', 'PRE_INDICATOR_SO_INDEX', 'Q_ACK', 'Q_BMM_ANNOUNCED',
                  'Q_BRAKE_CAPT_TYPE', 'Q_BTM_ALARM', 'Q_BUTTON[0]', 'Q_CAB_B', 'Q_DISPLAY_IS', 'Q_DISPLAY_PS',
                  'Q_DISPLAY_RS', 'Q_DISPLAY_TD', 'Q_DISPLAY_TS', 'Q_LINK', 'Q_MEDIA', 'Q_RBCENTRY', 'Q_SCALE',
                  'Q_SERVICEBRAKEFEEDBACK', 'Q_SERVICEBRAKEINTERFACE', 'Q_SOUND[0]', 'Q_SPECADDBRAKEINDADH',
                  'Q_TRACTIONCUTOFFINTERFACE', 'Q_UPDOWN', 'REFERENCE_BRAKE_PRESSURE',
                  'SEMI_EQUIPPED_RESTRICTIONS_SECTION', 'SEMI_EQUIPPED_RESTRICTION_COUNT', 'SOFT_BRAKE_APPLIED',
                  'SPEED_AND_DISTANCE_SUMMARY_SECTION', 'STM_AREAS_SECTION', 'STM_MODE', 'STM_SYSTEM_STATUS_MESSAGE',
                  'STOP_LANDSLIDE_PASSAGE_SECTION', 'SUPERVISE_BRAKES_SECTION', 'SUPERVISING_LANDSLIDE_PASSAGE_STATE',
                  'SUPERVISING_STOP_PASSAGE_STATE', 'SUPERVISION', 'SUPERVISION_OBJECTS_SUMMARY_SECTION',
                  'SYSTEM_STATUS_MESSAGE', 'SYSTEM_VERSION', 'TIME.HOUR', 'TIME.MILLISECONDS', 'TIME.MINUTES',
                  'TIME.SECONDS', 'TRAIN_DATA_SECTION', 'TRAIN_LENGTH_DELAYS_SECTION', 'TRAIN_LENGTH_DELAY_COUNT',
                  'TRAIN_POSITION.D_LRBG', 'TRAIN_POSITION.L_DOUBTOVER', 'TRAIN_POSITION.L_DOUBTUNDER',
                  'TRAIN_POSITION.NID_BG', 'TRAIN_POSITION.NID_C', 'TRAIN_POSITION.Q_DIRLRBG',
                  'TRAIN_POSITION.Q_DLRBG', 'TRAIN_POSITION.Q_SCALE', 'T_B', 'T_BRAKE_SERVICE(0)',
                  'T_BRAKE_SERVICE(1)', 'T_BRAKE_SERVICE(2)', 'T_BRAKE_SERVICE(3)', 'T_BRAKE_SERVICE(4)',
                  'T_BRAKE_SERVICE(5)', 'T_BRAKE_SERVICE(6)', 'T_BRAKE_SERVICE(7)', 'T_BUTTONEVENT[0]',
                  'T_ETCS_EBCHK', 'T_JD', 'T_TRACTION_CUT_OFF', 'T_TRAIN', 'T_X', 'V_DARK_ACTIVE', 'V_DARK_MAX_SPEED',
                  'V_DEC_ACTIVE', 'V_DEC_MAX_SPEED', 'V_ERR_ACTIVE', 'V_ERR_MAX_SPEED', 'V_ETCS_ACTIVE',
                  'V_ETCS_MAX_SPEED', 'V_HSI_ACTIVE', 'V_HSI_MAX_SPEED', 'V_HT_ET_ACTIVE', 'V_HT_ET_MAX_SPEED',
                  'V_HT_K1_ACTIVE', 'V_HT_K1_MAX_SPEED', 'V_HT_K2_ACTIVE', 'V_HT_K2_MAX_SPEED', 'V_HT_PT_01_ACTIVE',
                  'V_HT_PT_01_MAX_SPEED', 'V_HT_PT_02_ACTIVE', 'V_HT_PT_02_MAX_SPEED', 'V_HT_PT_03_ACTIVE',
                  'V_HT_PT_03_MAX_SPEED', 'V_HT_PT_04_ACTIVE', 'V_HT_PT_04_MAX_SPEED', 'V_HT_PT_05_ACTIVE',
                  'V_HT_PT_05_MAX_SPEED', 'V_HT_PT_06_ACTIVE', 'V_HT_PT_06_MAX_SPEED', 'V_HT_PT_07_ACTIVE',
                  'V_HT_PT_07_MAX_SPEED', 'V_HT_PT_08_ACTIVE', 'V_HT_PT_08_MAX_SPEED', 'V_HT_PT_09_ACTIVE',
                  'V_HT_PT_09_MAX_SPEED', 'V_HT_SK_ACTIVE', 'V_HT_SK_MAX_SPEED', 'V_HT_V1_ACTIVE',
                  'V_HT_V1_MAX_SPEED', 'V_HT_V2_ACTIVE', 'V_HT_V2_MAX_SPEED', 'V_HT_V3_ACTIVE', 'V_HT_V3_MAX_SPEED',
                  'V_INTERV', 'V_LINE_HT_G_ACTIVE', 'V_LINE_HT_G_MAX_SPEED', 'V_LINE_HT_T_ACTIVE',
                  'V_LINE_HT_T_MAX_SPEED', 'V_MAX', 'V_MAXTRAIN', 'V_PERM', 'V_PERMIT', 'V_RELEASE',
                  'V_REVERSE_ACTIVE', 'V_REVERSE_MAX_SPEED', 'V_SBI', 'V_SEMI_ACTIVE', 'V_SEMI_MAX_SPEED',
                  'V_START_ACTIVE', 'V_START_MAX_SPEED', 'V_STM_ACTIVE', 'V_STM_MAX', 'V_STM_MAX_SPEED', 'V_TARGET',
                  'V_TRAIN', 'WARNING_BOARDS_SECTION', 'WARNING_BOARD_COUNT', 'X_CAPTION(L_CAPTION)[0]',
                  'X_CAPTION(L_CAPTION)[1]', 'X_CAPTION(L_CAPTION)[2]', 'X_CAPTION(L_CAPTION)[3]',
                  'X_CAPTION(L_CAPTION)[4]', 'X_CAPTION(L_CAPTION)[5]', 'X_CAPTION(L_CAPTION)[6]',
                  'X_CAPTION(L_CAPTION)[7]', 'X_CAPTION(L_CAPTION)[8]', 'X_TEXT', 'X_TEXT(L_TEXT)', 'niter']

etcs_attributes = ['CURRENT_SPEED_1KPH', 'JRU_L_DATA', 'NID_PROPRIO_MESSAGE', 'TRU_L_TEXT', 'TRU_NID_SOURCE',
                   'TRU_Q_TEXT', 'TRU_Q_TEXTCLASS', 'TRU_Q_TEXTCONFIRM', 'TRU_X_TEXT']

# Init db with attributes fetch from previous adru files
initialize_adru_database(
    db_file,
    jru_attributes,
    etcs_attributes
)


def run_adru_txt_conversion():
    # Scan adru input directory for files
    adru_files = find_adru_files(adru_input_dir)

    for adru_file in adru_files:
        # Input file setup
        input_path = adru_file
        output_txt_path = Path(txt_output_dir)
        output_csv_path = Path(csv_output_dir)

        # Ensure output directory exists
        Path(output_txt_path).mkdir(parents=True, exist_ok=True)
        Path(output_csv_path).mkdir(parents=True, exist_ok=True)

        # Debug print
        print(f"🧪 Input file resolved: {input_path}")
        print("📄 File exists?", input_path.exists())

        # Fetch data from DB if file
        adru_file_id = add_adru_file_to_db(db_file, input_path)

        # Storage for output files variable
        previous_txt_file = get_latest_txt_file(output_txt_path)
        previous_time = previous_txt_file.stat().st_ctime if previous_txt_file else 0

        if previous_txt_file:
            print(f"✅ Found previous .txt file: {previous_txt_file.name} (Last modified: {previous_time})")

        # Check if adru_file has known txt file
        if not exist_txt_file_for_adru(adru_file_id, db_file, output_txt_path):
            print(f"❌ No known .txt file for ADRU file {adru_file.name}. Starting processing.")

            # Run for each output type (e.g., txt and csv)
            for ot in output_types:

                # Fetch output folder
                if ot == "t":
                    output_path_target = output_txt_path
                elif ot == "c":
                    output_path_target = output_csv_path
                else:
                    print(f"⚠️ Unknown output type: {ot}")
                    continue

                exe_file = Path(exe_path)
                command = rf"""
                Push-Location "{exe_file.parent}"
                .\{exe_file.name} -i "{input_path.parent}" `
                -o "{output_path_target}" `
                -f "{input_path.name}" `
                -it {input_type}  -ot {ot}
                Pop-Location
                """

                # Prompt user and fetch new path
                try:
                    new_txt = prompt_user_and_wait_for_txt(command, output_txt_path)
                    add_message_file_to_db(db_file, new_txt, adru_file_id)
                except TimeoutError as e:
                    print(f"❌ {e}")
                    exit(1)
        else:
            print(f"✅ known .txt file for ADRU file {adru_file.name} found. No need to create one.")

        # Fetch txt file for adru file
        amf_md5, newest_txt_file_path, total_messages, amf_id = fetch_newest_txt_file_for_adru(adru_file_id, db_file,
                                                                                               output_txt_path)

        if not newest_txt_file_path:
            print(f"❌ No .txt files found in {txt_output_dir}")
        else:
            # Find all the unique attribute lines and present them, this is only as a control
            # The db has already a preset of attribute, and if you see a new one in the list this needs to be manually
            # added to the database schema and the code that handles the attributes.
            jru_attrs, etcs_attrs = extract_unique_attributes(newest_txt_file_path, total_messages)

            # Compare with master attribute lists
            missing_jru = sorted(set(jru_attrs) - set(jru_attributes))
            missing_etcs = sorted(set(etcs_attrs) - set(etcs_attributes))

            if missing_jru:
                print("\n⚠️ Missing JRU attributes:")
                for attr in missing_jru:
                    print("  -", attr)
            else:
                print("✅ All JRU attributes are already in the database schema.")

            if missing_etcs:
                print("\n⚠️ Missing ETCS attributes:")
                for attr in missing_etcs:
                    print("  -", attr)
            else:
                print("✅ All ETCS attributes are already in the database schema.")

            # Verify that the txt file content has not all ready been added to the database
            if is_txt_content_in_db_with_entries(db_file, amf_id):
                print(f"✅ All messages from {newest_txt_file_path.name} are already in the database.")
                continue

            # Read the file and for each MSG add a row to it in the database
            insert_messages_from_txt(newest_txt_file_path, db_file, amf_id, total_messages)


def run_csv_txt_merge_conversion():
    # Scan csv raw for csv files
    all_csv_files = list(Path(csv_raw_dir).glob("*.csv"))

    if not all_csv_files:
        print("\n❌ No CSV files found in the input directory.")
        print(f"📁 Please add CSV files to this folder: {csv_raw_dir}")
        print("🔙 Returning to main menu.\n")
        return

    # Create menu to choose witch csv file to merge with .txt and validate that the number exist
    print("\n📄 Available CSV files:")
    for i, csv_file in enumerate(all_csv_files, start=1):
        print(f"{i}. {csv_file.name}")

    csv_choice = None
    while csv_choice not in [str(i) for i in range(1, len(all_csv_files) + 1)] and csv_choice != 'q':
        csv_choice = input("\nEnter the number of the CSV file to merge (or 'q' to quit): ").strip()

    if csv_choice == 'q':
        print("🔙 Merge cancelled. Returning to main menu.")
        return

    # Get selected CSV file
    selected_csv = all_csv_files[int(csv_choice) - 1]

    # Fetch ADRU files with known txt files
    adru_files_list = get_all_adru_files_that_has_txt_files_and_latest_txt(db_file)

    if not adru_files_list:
        print("\n❌ No ADRU .txt files found.")
        print("🛠 Please run the ADRU decoder first using option 1 in the main menu.")
        print("🔙 Returning to main menu.\n")
        return

    # Show available ADRU options
    print("\n📂 Available ADRU files with known .txt files:")
    for i, adru_file in enumerate(adru_files_list, start=1):
        print(f"{i}. {adru_file['file_name']}")

    adru_choice = None
    while adru_choice not in [str(i) for i in range(1, len(adru_files_list) + 1)] and adru_choice != 'q':
        adru_choice = input("\nEnter the number of the ADRU file to merge with the CSV (or 'q' to quit): ").strip()

    if adru_choice == 'q':
        print("🔙 Merge cancelled. Returning to main menu.")
        return

    # Fetch selected ADRU file
    selected_adru = adru_files_list[int(adru_choice) - 1]

    # Read selected CSV file
    df = pd.read_csv(selected_csv, delimiter=";", index_col=False)

    # Drop the first column if it's empty (it seems like it's just a placeholder)
    if df.columns[0] == '' or df.columns[0] == 'Unnamed: 0' or df.columns[0].startswith('Unnamed'):
        df.drop(columns=df.columns[0], inplace=True)

    # Fetch data from db for the selected ADRU file based on the entries in the csv
    updated_df = enrich_dataframe_with_db_values(df, db_file, selected_adru['file_id'])

    # Drop the columns that are completely empty
    print("🛠 Removing empty cells.")
    updated_df = updated_df.dropna(axis=1, how='all')

    # Debug print
    print(updated_df.columns.tolist())

    # Ensure output directory exists
    Path(csv_output_dir).mkdir(parents=True, exist_ok=True)

    # Save new merged file in the csv_out dir
    print(f"\n📂 Saving merged CSV to: {csv_output_dir}")
    output_path = Path(csv_output_dir) / f"merged_{selected_csv.name}"
    updated_df.to_csv(output_path, index=False, sep=";")
    print(f"✅ Merged CSV saved successfully: {output_path.name}")


def show_main_menu():
    print("\n====== ADRU Decoder Menu ======\n")
    print("1. 🧾 Generate .txt from .adru and insert into DB")
    print("2. 📎 Merge CSV with .txt (Coming soon)")
    print("3. ❌ Exit")

    choice = input("\nEnter your choice (1-3): ").strip()

    if choice == "1":
        run_adru_txt_conversion()
    elif choice == "2":
        run_csv_txt_merge_conversion()
    elif choice == "3":
        print("\n👋 Exiting program. Goodbye!")
    else:
        print("❌ Invalid choice. Please enter 1, 2, or 3.")


if __name__ == "__main__":
    show_main_menu()
