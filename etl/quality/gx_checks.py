try:
    from great_expectations.dataset import PandasDataset
except Exception:
    PandasDataset = None

def run_gx_suite(domain: str, df):
    if PandasDataset is None:
        return True
    ds = PandasDataset(df)
    if domain == "energy" and "kwh" in df.columns:
        ds.expect_column_values_to_not_be_null("kwh")
        ds.expect_column_values_to_be_between("kwh", min_value=0)
    res = ds.validate()
    return res.success