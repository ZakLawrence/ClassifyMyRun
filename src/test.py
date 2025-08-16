from pathlib import Path

parent_folder = Path(__file__).resolve().parent.parent

print(parent_folder/"token.json")
print(type(parent_folder))

def print_path(path:str):
    print(path)

print_path(parent_folder)