import yaml

def load_yaml(path: str):
    with open(path, "r") as f:
        return yaml.safe_load(f)