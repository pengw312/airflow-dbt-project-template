import os
from include.eczachly.snowflake_queries import get_snowpark_session
schema = os.getenv("SCHEMA")
import pandas as pd
def load_csv():
    session = get_snowpark_session()
    pdf = pd.read_csv(
        "include/eczachly/scripts/snowpark/zachs_posts.csv",
        sep=",",  # explicit delimiter
        engine="python",  # tolerant of embedded newlines in quoted fields
        quotechar='"',  # " ... " encloses a field
        doublequote=True,  # ""  -> literal "   (matches your sample)
        skip_blank_lines=False,  # keep any intentional empty lines inside quotes
        parse_dates=["Date"],
        on_bad_lines='skip',
        dtype={
            "ShareLink": "string",
            "SharedURL": "string",
            "MediaURL": "string",
            "Visibility": "string",
        },
        keep_default_na=False  # don’t turn empty cells into NaN unless you want to
    )
    session.create_dataframe(pdf).write.mode("overwrite").save_as_table("bootcamp.linkedin_shares")
    print(f"✅ Loaded {len(pdf):,} rows from 'zachs_posts.csv' into LINKEDIN_SHARES")
    session.close()








load_csv()

