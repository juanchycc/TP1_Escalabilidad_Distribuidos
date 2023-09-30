import os

output_files = ["out_file_q1.csv"]

for o in output_files:
    file_path = "./file_writer/" + o
    if os.path.exists(file_path):
        os.remove(file_path)
    file = open(file_path, "w")
