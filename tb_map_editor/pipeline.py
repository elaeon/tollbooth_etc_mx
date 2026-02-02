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

    def simple_pub_stg(self, model_name: str, year: int, normalize: bool):
        model_dict = self._get_model(model_name, year)
        print(f'Scan file: {model_dict["start"].csv}')
        schema = model_dict["start"].schema
        if "info_year" in schema:
            del schema["info_year"]
        
        ldf = pl.scan_csv(model_dict["start"].csv, infer_schema=True)
        ldf = ldf.cast(schema)
        ldf = ldf.pipe(self._simple_stg, model=model_dict["start"], normalize=normalize)
        ldf.sink_parquet(model_dict["end"].parquet)
        print(f'Sink file: {model_dict["end"].parquet}')

    def simple_raw_stg(self, model_name: str, year: int, file_path: str, old_fields: list, date_columns: dict | None = None, filter_exp: pl.Expr | None = None):
        model_dict = self._get_model(model_name, year)
        schema = model_dict["start"].schema
        field_map = {}
        for old_name, new_name in zip(old_fields, schema.keys()):
            field_map[old_name] = new_name

        ldf = pl.scan_csv(file_path, infer_schema=False)
        ldf = ldf.rename(field_map)
        if date_columns is not None:
            pl_date_exp = []
            for date_column, date_format in date_columns.items():
                pl_date_exp.append(pl.col(date_column).str.to_date(date_format))
            ldf = ldf.with_columns(pl_date_exp)

        if filter_exp is not None:
            ldf.filter(filter_exp)

        ldf = ldf.pipe(self._simple_stg, model=model_dict["start"])
        ldf = ldf.cast(schema)
        ldf.select(list(schema.keys())).sink_parquet(model_dict["end"].parquet)
