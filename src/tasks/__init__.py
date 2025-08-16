import pkgutil
import importlib
import pathlib

for module_info in pkgutil.iter_modules([str(pathlib.Path(__file__).parent)]):
    importlib.import_module(f"{__name__}.{module_info.name}")