import pandas as pd
from pandas_profiling import ProfileReport

if __name__ == "__main__":  # pragma: no cover
    df = pd.read_json("aklamio_challenge.json", lines=True)

    df.describe(include="all")

    profile = ProfileReport(df, title="Pandas Profiling Report")

    # Generate a pandas profiling report to have an insight on the json file
    profile.to_file("aklamio_report.html")
