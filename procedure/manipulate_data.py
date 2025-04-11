import pandas as pd
import numpy as np
import snowflake.snowpark as snowpark

def double_the_table(session: snowpark.Session):
  df = session.table("MY_GIT_TABLE").to_pandas()
  df['NEW_COLUMN'] = 'Making a change!!'

  session.write_pandas(df = df, schema="PUBLIC", table_name="NEW_GIT_TABLE", overwrite=True, auto_create_table=True)
  return "SUCCESS"
