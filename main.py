import pandas as pd
import os
import pathlib  # to find file extension
import shutil  # to copy files
# tests were done with pyarrow as the parquet engine, <pip install pyarrow>


# converts a folder of csv files to parquet files
def csv_to_parquet(file_name):
    print(f"\nProcessing: {file_name}")

    input_path = f"{INPUT_CSV_LOCATION}{file}"
    if pathlib.Path(file).suffix.lower() == '.csv':
        output_path = f"{OUTPUT_PARQUET_LOCATION}{file.replace('.csv','').replace('.CSV', '')}.parquet"

        try:
            df = pd.read_csv(input_path)
            try:
                df.to_parquet(output_path, engine="pyarrow", index=False)
                print(f"----- Converted to parquet")
            except Exception as e:
                print(f"----------------Error converting to parquet | {e}")
        except Exception as e:
            print(f"-----------------Error reading csv | {e}")
    else:
        print(f"----- Not a csv file, copying file directly")
        shutil.copy(input_path, OUTPUT_PARQUET_LOCATION)


# Coverts a folder of parquet files to csv files
def parquet_to_csv(file_name):
    print(f"\nProcessing: {file_name}")

    input_path = f"{INPUT_PARQUET_LOCATION}{file}"
    if pathlib.Path(file).suffix.lower() == '.parquet':
        output_path = f"{OUTPUT_CSV_LOCATION}{file.replace('.parquet', '.csv')}"

        try:
            df = pd.read_parquet(input_path)
            try:
                df.to_csv(output_path, index=False)
                print(f"----- Converted to csv")
            except Exception as e:
                print(f"----------------Error converting to csv | {e}")
        except Exception as e:
            print(f"-----------------Error reading parquet | {e}")
    else:
        print(f"----- Not a parquet file, copying file directly")
        shutil.copy(input_path, OUTPUT_CSV_LOCATION)


if __name__ == "__main__":
    # --------------------------------------------------------------------------------------
    # used absolute paths here since we have different operating systems
    # example file path: "C:/Apache-Parquet/input-data/"
    # don't forget the '/' at the end of the file path!

    # csv file input location and parquet file output locations
    INPUT_CSV_LOCATION = "C:/Apache-Parquet/input-data/"
    OUTPUT_PARQUET_LOCATION = "C:/Apache-Parquet/output-parquet/"

    # parquet file input location and csv output locations
    INPUT_PARQUET_LOCATION = "C:/Apache-Parquet/output-parquet/"
    OUTPUT_CSV_LOCATION = "C:/Apache-Parquet/output-csv/"
    # --------------------------------------------------------------------------------------

    # Checks for input data existence
    if not os.path.exists(INPUT_CSV_LOCATION) and not os.path.exists(INPUT_PARQUET_LOCATION):
        print("Error! No input data of any kind!")
        exit()

    # Converts csv data to parquet files
    if INPUT_CSV_LOCATION:
        if not OUTPUT_PARQUET_LOCATION:
            print("Error! No output parquet location!")
            exit()
        elif not os.path.exists(OUTPUT_PARQUET_LOCATION):
            os.makedirs(OUTPUT_PARQUET_LOCATION)

        for file in os.listdir(INPUT_CSV_LOCATION):
            csv_to_parquet(file)

    # Reads the parquet files and converts back to csv files
    if INPUT_PARQUET_LOCATION:
        if not OUTPUT_CSV_LOCATION:
            print("Error! No output csv location!")
            exit()
        elif not os.path.exists(OUTPUT_CSV_LOCATION):
            os.makedirs(OUTPUT_CSV_LOCATION)

        for file in os.listdir(INPUT_PARQUET_LOCATION):
            parquet_to_csv(file)
