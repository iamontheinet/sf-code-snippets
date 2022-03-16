import json

def get_dummies(df):
    # We will assume that all string columns are categorical variables
    cat_cols = [c.name for c in df.schema.fields if type(c.datatype) == T.StringType]
    # Get distinct values for each column
    dist_vals = df.select([F.call_builtin("strtok_to_array", F.listagg(col, ",", True), ",").as_(col) for col in cat_cols]).collect()[0].as_dict()
    
    for col in cat_cols:
        uniq_vals = json.loads(dist_vals[col])
        col_names = [col + '_' + val for val in uniq_vals]
        df = df.with_columns(col_names, [F.iff(F.col(col) == val, F.lit(1), F.lit(0)) for val in uniq_vals])
    df = df.drop(cat_cols)
    return df
