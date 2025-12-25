import os, sys
sys.path.append(os.path.dirname(os.path.realpath("tb_map_editor")))

from tb_map_editor.model import Strech
import polars as pl


assert Strech.dict_schema() == {"strech_id": pl.UInt16, "strech_name": pl.String}
