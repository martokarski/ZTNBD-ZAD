import logging
import os
import re

LOGGER = logging.getLogger(__file__)
MODULE_NAME_REGEX = re.compile(r"^stage\d{2}$")


class TransformerModuleManager:
    def __init__(self, modules_directory):
        module_directories = [mod for mod in os.listdir(modules_directory) if
                              os.path.isdir(os.path.join(modules_directory, mod))
                              and MODULE_NAME_REGEX.match(mod)]

        module_directories_sorted = sorted(module_directories)
        self._loaded_transformers = []
        self._loaded_transformer_names = []
        for mod_name in module_directories_sorted:
            print('.'.join((modules_directory, mod_name)))
            mod = __import__('.'.join((modules_directory, mod_name)), globals(), locals(), [mod_name])
            if hasattr(mod, "PipelineTransformer") and hasattr(mod, "NAME"):
                LOGGER.info("Loading module named '{}'.".format(mod.NAME))
                self._loaded_transformers.append(mod.PipelineTransformer)
                self._loaded_transformer_names.append(mod.NAME)
            else:
                raise ImportError("Wrong interface for module {}".format(mod_name))

    @property
    def loaded_transformers(self):
        return self._loaded_transformers

    @property
    def loaded_transformers_names(self):
        return self._loaded_transformer_names


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    mod_manager = TransformerModuleManager("modules")
    print(mod_manager.loaded_transformers_names)
