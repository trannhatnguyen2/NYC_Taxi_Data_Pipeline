import yaml

def load_cfg(cfg_file):
    """
    Load configuration from a YAML config file
    """
    cfg = None
    with open(cfg_file, "r") as f:
        try:
            cfg = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            print(exc)

    return cfg
