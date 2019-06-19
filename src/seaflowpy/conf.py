import configparser
import os
from . import util


CONFIG_FILE = os.path.expanduser("~/.seaflowpy/config")


def get_config(config_path=CONFIG_FILE):
    config = configparser.ConfigParser()
    _ = config.read(config_path)
    return config


def get_ssh_config(config=None, config_path=CONFIG_FILE):
    # Get input for SSH section
    section = "ssh"
    dirty = False

    if not config:
        config = get_config(config_path=config_path)

    if not config.has_section(section):
        config.add_section(section)
        dirty = True
    if not config.has_option(section, "ssh-private-key-file"):
        response = input("SSH private key location: ")
        config.set(section, "ssh-private-key-file", response)
        dirty = True
    if not config.has_option(section, "ssh-user"):
        response = input("Remote Linux user: ")
        config.set(section, "ssh-user", response)
        dirty = True

    if dirty:
        save_config(config, config_path)

    return config

def get_aws_config(config=None, config_path=CONFIG_FILE, s3_only=False):
    section = "aws"
    dirty = False

    if not config:
        config = get_config(config_path=config_path)

    if not config.has_section(section):
        config.add_section(section)
        dirty = True

    options = ["s3-bucket"]
    if not s3_only:
        options.extend(["ssh-private-key-name", "security-group", "image-id"])

    for o in options:
        if not config.has_option(section, o):
            response = input("{}: ".format(o))
            config.set(section, o, response)
            dirty = True

    if dirty:
        save_config(config, config_path)

    return config


def save_config(config, config_path=CONFIG_FILE):
    # Write config data to disk
    util.mkdir_p(os.path.dirname(config_path))
    with open(config_path, mode="w", encoding="utf=8") as fh:
        config.write(fh)
