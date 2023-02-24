import os
import pandas as pd
import sqlite3


def change_file_extension(path, from_extension, to_extension):
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(from_extension):
                old_path = os.path.join(root, file)
                new_path = os.path.splitext(old_path)[0] + to_extension
                shutil.move(old_path, new_path)


def list_files_with_extension(path, extension):
    db_list = []
    for file in os.listdir("./"):
        if file.endswith(".csv"):
            db_list.append(file)
    return db_list


def convert_n_csv_in_directory_to_n_sqlite_db(csv_dir, db_dir, separators=()):
    csv_list = list_files_with_extension(csv_dir, ".csv")

    for i, file in enumerate(csv_list):
        name = file.split('.')[0]
        if not os.path.isfile(f'./{name}.db'):
            db_path = os.path.join(db_dir, name + '.db')
            connection = sqlite3.connect(db_path)
            c = connection.cursor()
            c.execute('''CREATE TABLE users (timestamp int, price float, volume float)''')

            print(f'Reading {file}')
            df = pd.read_csv(file, sep='|'.join(separators))

            print(f'{file} read SUCCESSFULLY into DataFrame!\n'
                  f'Writing DataFrame into SQL database.')

            df.to_sql(name, connection, if_exists='append', index=False)

            print(f'Data of {file} has been written into {name}.db database.\n'
                  f'{((i + 1) / len(csv_list)) * 100} % is complete.')
        else:
            print(f'Database {name}.db already exist!')


# if __name__ == '__main__':
    # Example usage
    '''
    convert_N_csv_in_directory_to_N_sqlite_db(csv_dir="../test_csv_folder/",
                                              db_dir="../../test_db_folder/",
                                              separators=(';', ',', '%', '\t'))
    '''