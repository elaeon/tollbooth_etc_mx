import polars as pl

from .data_files import PathModel


class DataPipeline:

    def _simple_stg(self, ldf: pl.LazyFrame, model: PathModel, normalize: bool | None = True) -> pl.LazyFrame:
        exp_list = []
        if normalize is True:
            exp_list.extend(model.str_normalize)
        exp_list.append(pl.lit(model.attr.get("year")).alias("info_year"))
        ldf = ldf.with_columns(exp_list)
        return ldf

    def simple_pub_stg(self, pub: PathModel, normalize: bool, date_columns: dict | None = None) -> pl.LazyFrame:
        print(f'Scan file: {pub.csv}')
        schema = pub.schema
        lf = pl.scan_csv(pub.csv, infer_schema_length=10000)
        if date_columns is not None:
            lf = lf.with_columns([pl.col(dc).str.strip_chars() for dc in date_columns])
            lf = lf.with_columns([pl.col(dc).str.to_date(fmt) for dc, fmt in date_columns.items()])
        lf = lf.pipe(self._simple_stg, model=pub, normalize=normalize)
        data_schema = lf.collect_schema()
        data_columns = set(data_schema.keys())
        model_columns = set(schema.keys())
        null_columns = model_columns.difference(data_columns)
        if null_columns:
            print("Warn: ", null_columns, f"does not exist in file {pub.csv}")
        lf = lf.with_columns([pl.lit(None).alias(col) for col in null_columns])
        lf = lf.cast(schema)
        return lf

    def simple_raw_stg(self, pub: PathModel, file_path: str, old_fields: list,
                       date_columns: dict | None = None, filter_exp: pl.Expr | None = None,
                       normalize: bool | None = True, extra_expr: list | None = None) -> pl.LazyFrame:
        schema = pub.schema
        field_map = {old: new for old, new in zip(old_fields, schema.keys())}
        lf = pl.scan_csv(file_path, infer_schema=False)
        if date_columns is not None:
            lf = lf.with_columns([pl.col(dc).str.to_date(fmt) for dc, fmt in date_columns.items()])
        if filter_exp is not None:
            lf = lf.filter(filter_exp)
        lf = lf.rename(field_map)
        if extra_expr is not None:
            lf = lf.with_columns(extra_expr)
        lf = lf.pipe(self._simple_stg, model=pub, normalize=normalize)
        lf = lf.cast(schema)
        return lf.select(list(schema.keys()))
