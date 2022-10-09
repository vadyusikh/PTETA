import pathlib

path = pathlib.Path("data_05Sep/")
files_list = list(path.iterdir())

files_list = sorted(files_list)

print(files_list[:100])