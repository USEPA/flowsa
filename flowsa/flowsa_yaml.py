from typing import IO, Any
import yaml
import flowsa.settings
from os import path


class FlowsaLoader(yaml.SafeLoader):
    '''
    Custom YAML loader implementing !include: tag to allow inheriting
    arbitrary nodes from other yaml files.
    '''
    def __init__(self, stream: IO, external_config_path: str = None) -> None:
        super().__init__(stream)
        self.add_multi_constructor('!include:', include)
        self.external_config_path = str(external_config_path)


def include(loader: FlowsaLoader, suffix: str, node: yaml.Node) -> dict:
    file, *keys = suffix.split(':')

    for folder in [
        loader.external_config_path,
        flowsa.settings.sourceconfigpath,
        flowsa.settings.flowbysectormethodpath
    ]:
        if path.exists(path.join(folder, file)):
            file = path.join(folder, file)
            break
    else:
        raise FileNotFoundError

    with open(file) as f:
        branch = load(f, loader.external_config_path)

    while keys:
        branch = branch[keys.pop(0)]

    if isinstance(node, yaml.MappingNode):
        if isinstance(branch, dict):
            context = loader.construct_mapping(node)
            branch.update(context)
        else:
            raise TypeError(f'{suffix} is not a mapping/dict')

    elif isinstance(node, yaml.SequenceNode):
        if isinstance(branch, list):
            context = loader.construct_sequence(node)
            branch.extend(context)
        else:
            raise TypeError(f'{suffix} is not a sequence/list')

    return branch


def load(stream: IO, external_config_path: str = None) -> dict:
    loader = FlowsaLoader(stream, external_config_path)
    try:
        return loader.get_single_data()
    finally:
        loader.dispose()
