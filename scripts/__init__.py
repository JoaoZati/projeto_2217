import os
import sys
from decouple import Config, RepositoryEnv

ABOVE_DIRECTORY_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.path.pardir)
)

sys.path.append(ABOVE_DIRECTORY_PATH)

DOTENV_FILE = os.path.join(ABOVE_DIRECTORY_PATH, '.env')
env_config = Config(RepositoryEnv(DOTENV_FILE))
