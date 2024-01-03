# import pandas as pd
# import csv

# def hash_partition(csv_file, num_partitions):
  
    

#     with open(csv_file, 'r') as f:
#         reader = csv.reader(f, delimiter=',')
#         data = list(reader)

#     partitions = []
#     for i in range(num_partitions):
#         partition = []
#         for row in data:
#             hash_value = hash(row[0]) % num_partitions
#             if hash_value == i:
#                 partition.append(row)
#         partitions.append(pd.DataFrame(partition))

#     return partitions

# if __name__ == '__main__':
#     csv_file = r'C:\Users\hp\Downloads\Store_File\User_Data.csv'
#     num_partitions = 10
#     partitions = hash_partition(csv_file, num_partitions)

#     for partition in partitions:

#         print(partition)









# import pandas as pd
# import csv

# def dynamic_hash_partition(csv_file, max_file_size):
    

#     with open(csv_file, 'r') as f:
#         reader = csv.reader(f, delimiter=',')
#         data = list(reader)

#     partitions = []
#     file_size = 0
#     partition = []
#     for row in data:
#         hash_value = hash(row[0]) % 10
#         if file_size + len(row) <= max_file_size:
#             partition.append(row)
#             file_size += len(row)
#         else:
#             partitions.append(pd.DataFrame(partition))
#             file_size = len(row)
#             partition = [row]

#     partitions.append(pd.DataFrame(partition))

#     return partitions

# if __name__ == '__main__':
#     csv_file = r'C:\Users\hp\Downloads\Store_File\records_1lac.csv'
#     max_file_size = 100000
#     partitions = dynamic_hash_partition(csv_file, max_file_size)

#     for partition in partitions:
#         print(partition)



# import pandas as pd
# import numpy as np

# def validate_records(df, batch_size=100000):
#     """
#     Validate the records in a dataframe in batches.

#     Args:
#         df (pd.DataFrame): The dataframe to validate.
#         batch_size (int, optional): The size of each batch. Defaults to 100000.

#     Returns:
#         bool: True if the validation was successful, False otherwise.
#     """

#     success = True
#     for i in range(0, df.shape[0], batch_size):
#         batch = df[i:i + batch_size]
#         if not batch.isnull().values.any():
#             success = False
#             break
#     return success


# if __name__ == "__main__":
#     df = pd.DataFrame({
#         "Name": np.random.choice(["", "Jane", "Peter", "Mary", "Paul"], size=1000000),
#         "Age": np.random.randint(20, 60, size=1000000)
#     })

#     success = validate_records(df)

#     if success:
#         print("Validation successful")
#     else:
#         print("Validation failed")



import pandas as pd
import csv

def hash_partition(csv_file, num_partitions):
  
    

    with open(csv_file, 'r') as f:
        reader = csv.reader(f, delimiter=',')
        data = list(reader)

    partitions = []
    for i in range(num_partitions):
        partition = []
        for row in data:
            hash_value = hash(row[0]) % num_partitions
            if hash_value == i:
                partition.append(row)
        partitions.append(pd.DataFrame(partition))

    return partitions

def add_partitions(partitions):
    new_partitions = []
    for i in range(0, len(partitions), 2):
        new_partition = pd.concat([partitions[i], partitions[i + 1]], ignore_index=True)
        new_partitions.append(new_partition)
    return new_partitions

if __name__ == '__main__':
    csv_file = r'C:\Users\hp\Downloads\Store_File\User_Data.csv'
    num_partitions = 10
    partitions = hash_partition(csv_file, num_partitions)

    new_partitions = add_partitions(partitions)

    for partition in new_partitions:

        print(partition) 