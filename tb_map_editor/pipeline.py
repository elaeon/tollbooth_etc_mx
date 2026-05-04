import polars as pl

from .data_files import DataModel, DataStage, PathModel


class DataPipeline:

    def _simple_stg(self, ldf: pl.LazyFrame, model: PathModel, normalize: bool = True) -> pl.LazyFrame:
        exp_list = []
        if normalize is True:
            exp_list.extend(model.str_normalize)
        
        exp_list.append(
            pl.lit(model.attr.get("year")).alias("info_year")
        )
        ldf = ldf.with_columns(exp_list)
        return ldf
    
    def _get_model(self, model_name: str, year: int) -> dict[str, PathModel]:
        data_model_start = DataModel(year, DataStage.pub)
        data_model_end = DataModel(year, DataStage.stg)
        model_dict = {
            "start": getattr(data_model_start, model_name),
            "end": getattr(data_model_end, model_name)
        }
        return model_dict

    def simple_pub_stg(self, model_name: str, year: int, normalize: bool, date_columns: dict | None = None):
        model_dict = self._get_model(model_name, year)
        print(f'Scan file: {model_dict["start"].csv}')
        schema = model_dict["start"].schema
        lf = pl.scan_csv(model_dict["start"].csv, infer_schema_length=10000)
        if date_columns is not None:
            pl_date_exp = []
            for date_column, date_format in date_columns.items():
                pl_date_exp.append(pl.col(date_column).str.strip_chars())
            lf = lf.with_columns(pl_date_exp)

            pl_date_exp = []
            for date_column, date_format in date_columns.items():
                pl_date_exp.append(pl.col(date_column).str.to_date(date_format))
            lf = lf.with_columns(pl_date_exp)
        
        lf = lf.pipe(self._simple_stg, model=model_dict["start"], normalize=normalize)
        data_schema = lf.collect_schema()
        data_columns = set(data_schema.keys())
        model_columns = set(schema.keys())
        null_columns = model_columns.difference(data_columns)
        if len(null_columns) > 0:
            print("Warn: ", null_columns, f"does not exist in file {model_dict["start"].csv}")
        lf = lf.with_columns(
            [pl.lit(None).alias(col) for col in null_columns]
        )
        lf = lf.cast(schema)
        return lf, model_dict["end"]

    def simple_raw_stg(self, model_name: str, year: int, file_path: str, old_fields: list, 
                       date_columns: dict | None = None, filter_exp: pl.Expr | None = None, 
                       normalize: bool | None = True, extra_expr: list | None = None):
        model_dict = self._get_model(model_name, year)
        schema = model_dict["start"].schema
        field_map = {}
        for old_name, new_name in zip(old_fields, schema.keys()):
            field_map[old_name] = new_name

        lf = pl.scan_csv(file_path, infer_schema=False)
        if date_columns is not None:
            pl_date_exp = []
            for date_column, date_format in date_columns.items():
                pl_date_exp.append(pl.col(date_column).str.to_date(date_format))
            lf = lf.with_columns(pl_date_exp)

        if filter_exp is not None:
            lf.filter(filter_exp)

        lf = lf.rename(field_map)
        if extra_expr is not None:
            lf = lf.with_columns(extra_expr)
        lf = lf.pipe(self._simple_stg, model=model_dict["start"], normalize=normalize)
        lf = lf.cast(schema)
        return lf.select(list(schema.keys())), model_dict
