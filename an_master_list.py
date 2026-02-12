def rename_with_schema(df, schema_csv_path):
    schema = pd.read_csv(schema_csv_path)

    expected_from = set(schema["from"])
    actual_cols = set(df.columns)

    # 1️⃣ Check that all expected columns exist
    missing = expected_from - actual_cols
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    # 2️⃣ Rename (order preserved as-is)
    rename_map = dict(zip(schema["from"], schema["to"]))
    df = df.rename(columns=rename_map)

    return df


import re 

def clean_phone_numbers(df, cols):
    """
    Normalize phone number columns in a DataFrame.

    Parameters:
    - df: pandas DataFrame
    - cols: list of column names to clean

    Behavior:
    - Removes extensions (ext, ext., x)
    - Removes all non-digit characters
    - Converts 10-digit numbers to 11-digit with leading 1
    - Keeps 11-digit numbers as-is
    - Invalid numbers become None
    """
    for col in cols:
        if col not in df.columns:
            continue  # skip missing columns safely

        df[col] = (
            df[col]
            .astype(str)  # in case there are numeric types
            .str.replace(r"(ext\.?|x)\s*\d+", "", regex=True, flags=re.IGNORECASE)
            .str.replace(r"\D", "", regex=True)
            .apply(lambda x: "1" + x if len(x) == 10 else (x if len(x) == 11 else None))
        )

    return df


import pandas as pd

# members and jobs table prep
#load csv to dfs
df_jobs = pd.read_csv("members_and_jobs.csv")
df_bu_codes = pd.read_csv("contract_codes_from_bu.csv")

# map bargaining unit codes
df_jobs["Contract Code"] = df_jobs["Bargaining Unit Code"].map(
    df_bu_codes.set_index("c28_bargaining_unit")["contract"])

# columns to keep
jobs_to_keep = ["Local Code", "Employee No.", "First Name", "Last Name", "Short #","Work Email", "Member Type", "Type Date", "Agency Code", "Job Class Code", "MoD Card", "Agency Name","Bargaining Unit Code","Contract Code","Building Code Name"]
df_jobs = df_jobs[jobs_to_keep]

df_jobs["Employee No."] = (
    pd.to_numeric(df_jobs["Employee No."], errors="coerce")
      .astype("Int64")
)



#check progress
df_jobs.head() #still needs member type to be abbreviated. Employee No. will be used for join but then left out of final list.

#positions table prep
df_positions = pd.read_csv("members_and_positions.csv")
df_positions = df_positions[df_positions["Position Active"].str.strip().str.lower() == "yes"]

#add Local Presidient
df_positions["Local President"] = pd.NA  # start with empty string
df_positions.loc[df_positions["Position Name"] == "Local President", "Local President"] = "Y"

# add eboard member
df_positions["Local Executive Board Member"] = pd.NA
df_positions.loc[df_positions["Position Name"] == "Local Executive Board Member", "Local Executive Board Member"] = "Y"

# add steward
df_positions["Steward"] = pd.NA
df_positions.loc[df_positions["Position Name"] == "Steward", "Steward"] = "Y"

# Policy delegate
df_positions["Policy Committee Delegate"] = pd.NA
df_positions.loc[df_positions["Position Name"] == "Policy Committee Delegate", "Policy Committee Delegate"] = "Y"

#drop columns
positions_to_keep = ["Short #", "Local President", "Local Executive Board Member", "Policy Committee Delegate", "Steward"]
df_positions = df_positions[positions_to_keep]

#mask for row filter
criteria_cols = [
    "Local President",
    "Local Executive Board Member",
    "Policy Committee Delegate",
    "Steward",
]

mask = (df_positions[criteria_cols] == "Y").any(axis=1)

df_positions = df_positions[mask]

df_positions = (
    df_positions
        .assign(**{c: df_positions[c].eq("Y") for c in criteria_cols})
        .groupby("Short #", as_index=False)[criteria_cols]
        .any()
        .replace({True: "Y", False: pd.NA})
)

#df_positions.head()

#people table prep
df_people = pd.read_csv("members_and_people.csv")





#filter down
people_to_keep = ["Short #", "PEOPLE Active"]
df_people = df_people[people_to_keep]

#Change from "Yes" to "Y". Assumes all are already "Yes" comming out of Unionware
df_people["PEOPLE Active"] = "Y"

#check progress
#df_people.head()

#work addresses prep
df_work_addresses = pd.read_csv("members_and_work_addresses.csv")
df_work_addresses.columns = df_work_addresses.columns.str.strip()

#check fields are all there and in order. duplicate names are given ".1" appendage by pandas currently 1-14-26
expected_order = ["Local Code", "Short #", "Employee No.", "First Name", "Middle Name", "Last Name", 
                  "Member Active", "Member Status", "Status Date", "Birth Date", "Gender", "Address Line 1", 
                  "Address Line 2", "City", "State Abbr.", "Zip Code", "Home Phone", "Cell Phone", 
                  "Work Phone", "Home Email", "Work Email", "AFSCME ID", "Member Type", "Type Date", 
                  "Card Signature Date", "Agency Code", "Agency Name", "Work Site Work Site Code", 
                  "Work Site Name", "Building Code Code", "Building Code Name", "Job Class Code", 
                  "Job Class Name", "Address Type",	"Address Line 1.1" ,"Address Line 2.1", "Address Line 3", 
                  "City.1", "County", "Job Work City Name", "Work County Name", "Seniority Date", 
                  "Policy Group Code",	"Policy Group Name", "Bargaining Unit Code", "Bargaining Unit Name", 
                  "Field Office Name", "Job ID"]

assert list(df_work_addresses.columns) == expected_order, "Unexpected column order check exports from Unionware for updates."


# concat address line 1 and line 2 with a space between. We may drop this because it looks like Michael was.
df_work_addresses["Work Address"] = df_work_addresses["Address Line 1.1"].str.cat(df_work_addresses["Address Line 2.1"], sep=" ", na_rep="")

df_work_addresses["Employee No."] = (
    pd.to_numeric(df_work_addresses["Employee No."], errors="coerce")
      .astype("Int64")
)

work_addresses_to_keep = ["Short #", "Employee No.", "Birth Date", "Policy Group Code", "Work Address", "Job Work City Name", "Work County Name", "Work Site Work Site Code", "Field Office Name", "Job ID"]

df_work_addresses = df_work_addresses[work_addresses_to_keep]

#df_work_addresses.head()



#members and phones and emails prep
df_phones_and_emails = pd.read_csv("members_and_phones_emails.csv")

df_phones_and_emails.loc[df_phones_and_emails["Email Allowed"] == "no", "Home Email"] = ""
df_phones_and_emails.loc[df_phones_and_emails["Email Allowed"] == "no", "Work Email"] = ""
df_phones_and_emails.loc[df_phones_and_emails["Email Allowed"] == "no", "External Email"] = ""
df_phones_and_emails.loc[df_phones_and_emails["Phone Allowed"] == "no", "Cell Phone"] = ""

phones_and_emails_to_keep = ["Short #", "Home Email", "Work Email", "External Email", "Cell Phone","Job ID"]

df_phones_and_emails = df_phones_and_emails[phones_and_emails_to_keep]
df_phones_and_emails.head()

df_addresses = pd.read_csv("members_and_addresses.csv")

mask = (
    df_addresses["Mail Allowed"].str.strip().str.lower().eq("no") |
    df_addresses["Bad Address"].str.strip().str.lower().eq("yes")
)
df_addresses.loc[mask, ["Address Line 1", "City", "State", "Zip Code"]] = ""

addresses_to_keep = ["Short #","First Name", "Last Name", "Address Line 1", "City", "State Abbr.", "Zip Code"]

df_addresses = df_addresses[addresses_to_keep]

df_addresses = df_addresses.sort_values(by="Short #")

df_addresses.head()

df_all = (
    df_jobs
    .merge(df_positions, how="left", on="Short #")
    .merge(df_work_addresses, how="left", on=["Employee No.", "Short #"])
)

#df_all.head()

df_all = (
    df_all
    .merge(df_phones_and_emails, how="left", on=["Short #", "Work Email", "Job ID"])
    .merge(df_people, how="left", on="Short #")
    .merge(df_addresses, how="left", on=["Short #", "First Name", "Last Name"])
)

df_all = df_all.drop(columns=["Employee No.", "Job ID"])

#df_all.head()

#normalize values. This can be refactored. will probably want to fill all NaN.
df_all["First Name"] = df_all["First Name"].str.lower().str.capitalize().fillna("")
df_all["Last Name"] = df_all["Last Name"].str.lower().str.capitalize().fillna("")
df_all["Home Email"] = df_all["Home Email"].str.lower().fillna("")
df_all["Work Email"] = df_all["Work Email"].str.lower().fillna("")
df_all["External Email"] = df_all["External Email"].str.lower().fillna("")

# Dedupe after merge
df_all = df_all.drop_duplicates()

phone_cols = ["Cell Phone"]
df_all = clean_phone_numbers(df_all, phone_cols)

# map bargaining unit codes
df_all = rename_with_schema(df_all, "final_header_map.csv")

# fill blanks in Position related fields with "N".
df_all["c28_local_president"] = df_all["c28_local_president"].fillna("N")
df_all["c28_council_eboard"] = df_all["c28_council_eboard"].fillna("N")
df_all["c28_policy_comm_delegate"] = df_all["c28_policy_comm_delegate"].fillna("N")
df_all["c28_steward"] = df_all["c28_steward"].fillna("N")
df_all["c28_people"] = df_all["c28_people"].fillna("N")

df_all.info()
df_all.to_csv("test_all.csv",index=False)