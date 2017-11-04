import logging
import os

LOGGER = logging.getLogger(__file__)


class ModuleManager:
    def __init__(self, modules_directory):
        module_directories = [mod for mod in os.listdir(modules_directory) if
                              os.path.isdir(os.path.join(modules_directory, mod))]
        self._loaded_modules = {}
        for mod_name in module_directories:
            print('.'.join((modules_directory, mod_name)))
            mod = __import__('.'.join((modules_directory, mod_name)), globals(), locals(), [mod_name], -1)
            if hasattr(mod, "MODULE_DESCRIPTION") and hasattr(mod, "run"):
                LOGGER.info("Loading module '{}', described as:\n{}.".format(mod_name, mod.MODULE_DESCRIPTION))
                self._loaded_modules[mod_name] = mod
            else:
                raise ImportError("Wrong interface for module {}".format(mod_name))

    @property
    def available_modules(self):
        return {name: mod for name, mod in self._loaded_modules.items()}

    def module_names(self):
        return self._loaded_modules.keys()

    def __getitem__(self, name):
        return self._loaded_modules[name]

    def __iter__(self):
        return self._loaded_modules.iterkeys()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    mod_manager = ModuleManager("modules")
    print(mod_manager.available_modules)
    for name in mod_manager:
        print("Working with module {}, described as '{}'".format(name, mod_manager[name].MODULE_DESCRIPTION))
        mod_manager[name].run("Hello, witold!")
