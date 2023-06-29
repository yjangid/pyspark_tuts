import yaml
import os

def load_yaml(file_name):
    script_directory = os.path.dirname(os.path.abspath(__file__))
    project_directory = os.path.dirname(script_directory)
    file_path = os.path.join(project_directory, "data_pipeline", f"{file_name}.yaml")
    with open(file_path, "r") as file:
        config = yaml.safe_load(file)

    return config


def main():
    module_name = os.path.basename(__file__)
    print(f"Module - {module_name}")

if __name__ == "__main__":
    main()