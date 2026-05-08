import os
import sys

sys.path.append(os.path.dirname(os.path.realpath("src")))

import polars as pl

from src.model import Strech

assert Strech.dict_schema() == {"strech_id": pl.UInt16, "strech_name": pl.String}
