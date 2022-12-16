import pandas as pd
import os

# tests were done with pyarrow as the parquet engine
# pip install pyarrow


def csv_to_parquet(file_name, input_path, output_path):
    print(f"\nReading in: {file_name}")

    try:
        df = pd.read_csv(input_path)
        try:
            df.to_parquet(output_path, engine="pyarrow")
            print(f"----- Converted to parquet")
        except Exception as e:
            print(f"----------------Error converting to parquet | {e}")
    except Exception as e:
        print(f"-----------------Error reading csv | {e}")


def parquet_to_csv(file_name, input_path, output_path):
    print(f"\nReading in: {file_name}")
    try:
        df = pd.read_parquet(input_path)
        try:
            df.to_parquet(output_path)
            print(f"----- Converted to csv")
        except Exception as e:
            print(f"----------------Error converting to csv | {e}")
    except Exception as e:
        print(f"-----------------Error reading parquet | {e}")


if __name__ == "__main__":

    # used absolute paths here since we have different operating systems
    input_csv = "C:/Apache-Parquet/input-data/"
    parquet_files = "C:/Apache-Parquet/output-parquet/"  # Parquet files converted from csv
    csv_files = "C:/Apache-Parquet/output-csv/"  # CSV files converted back from csv

    if not os.path.exists(input_csv):
        print("Error! No Input data!")
        exit()
    if not os.path.exists(parquet_files):
        os.makedirs(parquet_files)
    if not os.path.exists(csv_files):
        os.makedirs(csv_files)

    # Converts csv data to parquet files
    for file in os.listdir(input_csv):
        input_file_path = f"{input_csv}{file}"
        output_file_path = f"{parquet_files}{file.replace('.csv','').replace('.CSV', '')}.parquet"

        csv_to_parquet(file, input_file_path, output_file_path)

    # Reads the parquet files and converts back to csv files
    for file in os.listdir(parquet_files):
        input_file_path = f"{parquet_files}{file}"
        output_file_path = f"{csv_files}{file.replace('.parquet', '')}.csv"

        parquet_to_csv(file, input_file_path, output_file_path)

    # -------------- Code below this line works, code above this line causes faulty csv files for some reason -------------------

    data = [['apple', 10], ['orange', 15], ['peach', 14]]

    df_test = pd.DataFrame(data, columns=['Fruit', 'Amount'])
    df_test.to_parquet("dataframe.parquet", engine="pyarrow")  # pip install pyarrow

    df_test_2 = pd.read_parquet("dataframe.parquet")
    df_test_2.to_csv("filename.csv")
