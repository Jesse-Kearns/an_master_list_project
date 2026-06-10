#!/usr/bin/env python
# coding: utf-8

# We may want to just generate several smaller lists, rather than the "Master" list we have been doing. we upload personal emails separete from work emails and there are separate lists for 443 
# and another local. We can programatically generate all 4 instead, since the current process is to break the "master" list into these 4 anyway. 
# 
# Will need to talk with Patrick about the requirements for each individual list. 
# 
# Add "work shift" to final output perhaps?
# 
# definitive way to determine if someone is an interpreter? we are tracking in AN.
# 
# be doubly sure that dates are indeed dates and not excel codes
# 
# double check TAMWU for contract codes in ouput
# 
# on load of csvs, impose dtypes. too many are object
# 

# there are two helpr functions in this notebook, one for renaming headers and one for cleaning phone numbers.
# 
# most of this script is to prepare the raw tables from Unionware for joins. 
# then we join the tables and write the csvs out. 
# what needs to be in the prepped tables before ther area joined may change as we rewrite because we want the focus of the joins to be on emails now instead of short #.
# each tables prep can be wrapped in a function to call in a master function for cleaner reading once this is converted to a .py script.
# 

# In[ ]:


import pandas as pd
import re


# In[ ]:


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
            continue  # skip missing columns safely. 

        df[col] = (
            df[col]
            .astype(str)  # in case there are numeric types
            .str.replace(r"(ext\.?|x)\s*\d+", "", regex=True, flags=re.IGNORECASE) #regex are wild. This removes phone extensions and non-digits
            .str.replace(r"\D", "", regex=True) #more regex to convert to digits?
            .apply(lambda x: "1" + x if isinstance(x, str) and len(x) == 10 else (x if isinstance(x, str) and len(x) == 11 else None))#honestly dont understand the lambda function here. Context says its a one-liner for putting a '1' on the front if its 10 characters long and if its 11 do nothing, invalid numbers become NONE.
        )

    return df


# In[ ]:


# members and jobs table prep
#load csv to dfs
df_jobs = pd.read_csv("inputs/members_and_jobs.csv")
df_bu_codes = pd.read_csv("helper_tables/contract_codes_from_bu.csv")

# map bargaining unit codes
df_jobs["Contract Code"] = df_jobs["Bargaining Unit Code"].map(
    df_bu_codes.set_index("c28_bargaining_unit")["contract"])

# columns to keep
jobs_to_keep = [
    "Birth Date",
    "AFSCME ID",
    "Local Code",
    "Employee No.",
    "Short #",
    "Member Type",
    "Home Email",
    "External Email",
    "Work Email",
    "Type Date",
    "Agency Code",
    "Job Class Code",
    "MoD Card",
    "Agency Name",
    "Bargaining Unit Code",
    "Building Code Name",
    "Contract Code",
    "Job Active"
]

df_jobs = df_jobs[jobs_to_keep]

df_jobs["Employee No."] = (
    pd.to_numeric(df_jobs["Employee No."], errors="coerce")
      .astype("Int64")
)

# rename member_type values to initials via mapping csv
df_map = pd.read_csv("helper_tables/member_type_map.csv")
map_series = df_map.set_index("from")["to"]
df_jobs["Member Type"] = df_jobs["Member Type"].map(map_series)

#melt/unpivot
df_personal_jobs = df_jobs.melt(
    id_vars = [
        "Birth Date",
        "AFSCME ID",
        "Local Code", 
        "Employee No.",
        "Member Type",
        "Type Date",
        "Agency Code",
        "Job Class Code",
        "MoD Card",
        "Agency Name",
        "Bargaining Unit Code",
        "Building Code Name",
        "Contract Code",
        "Job Active"
    ],
    value_vars = ["Home Email", "External Email"],
    value_name = "Email").dropna(subset=["Email"]).drop(columns=["variable"])
df_personal_jobs = df_personal_jobs[df_personal_jobs['Job Active'] != "FALSE"]
df_personal_jobs["Email"] = df_personal_jobs["Email"].str.lower()
df_personal_jobs = df_personal_jobs.drop_duplicates(["Email"])



df_work_jobs = df_jobs.drop(columns=["Short #", "External Email", "Home Email"])
df_work_jobs = df_work_jobs[df_work_jobs['Job Active'] != "FALSE"]
df_work_jobs = df_work_jobs.sort_values(by="Type Date")
df_work_jobs["Work Email"] = df_work_jobs["Work Email"].str.lower()
df_work_jobs = df_work_jobs.drop_duplicates("Work Email", keep='last')


#check progress
#df_personal_jobs.head() 

df_work_jobs.info()


# In[ ]:


# Members and Contracts

#load table and helper table for map
df_contracts = pd.read_csv("inputs/members_and_contracts.csv")
df_contract_codes = pd.read_csv("helper_tables/contract_codes_from_names.csv")

# Map codes from names 
df_contracts["Contract Code"] = df_contracts["Contract.1"].map(
    df_contract_codes.set_index("contract")["contract code"])

contracts_to_keep = ["Short #", "Contract Code"]
df_contracts = df_contracts[contracts_to_keep]
df_contracts.head()


# In[ ]:


#positions table prep
df_positions = pd.read_csv("inputs/members_and_positions.csv")
df_positions = df_positions[df_positions["Position Active"].str.strip().str.lower() == "TRUE"]

#add Local Presidient field
df_positions["Local President"] = pd.NA  # start with empty string
df_positions.loc[df_positions["Position Name"] == "Local President", "Local President"] = "Y"

# add eboard member field
df_positions["Local Executive Board Member"] = pd.NA
df_positions.loc[df_positions["Position Name"] == "Local Executive Board Member", "Local Executive Board Member"] = "Y"

# add steward field
df_positions["Steward"] = pd.NA
df_positions.loc[df_positions["Position Name"] == "Steward", "Steward"] = "Y"

# Policy delegate field
df_positions["Policy Committee Delegate"] = pd.NA
df_positions.loc[df_positions["Position Name"] == "Policy Committee Delegate", "Policy Committee Delegate"] = "Y"

# Local Officer 
df_positions["Local Officer"] = pd.NA
df_positions.loc[df_positions["Position Name"].isin(["Local Vice-President", "Local Secretary", "Local Secretary-Treasurer", "Local President", "Local Recording Secretary", "Local 1st Vice-President", "Local Treasurer", "Local Executive Vice President", "Local 2nd Vice-President", "Local Corresponding Secreatary"]), "Local Officer"] = "Y" 

#drop columns
positions_to_keep = ["Short #", "Local President", "Local Executive Board Member", "Policy Committee Delegate", "Steward", "Local Officer"] #add in "Local Officer"
df_positions = df_positions[positions_to_keep]

#mask for row filter
criteria_cols = [
    "Local President",
    "Local Executive Board Member",
    "Policy Committee Delegate",
    "Steward",
    "Local Officer",
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


df_positions.head()


# In[ ]:


#people table prep
df_people = pd.read_csv("inputs/members_and_people.csv")



#filter down
people_to_keep = ["Short #", "PEOPLE Active"]
df_people = df_people[people_to_keep]

#Change from "TRUE" to "Y". Assumes all are already "TRUE" comming out of Unionware
df_people["PEOPLE Active"] = "Y"
df_people = df_people.drop_duplicates(["Short #"])
df_people["PEOPLE Active"] = df_people["PEOPLE Active"].fillna("N")

#check progress
df_people.info()


# In[ ]:


#work addresses prep
#relies on Unionware list having already been filtered to prefered address only

df_work_addresses = pd.read_csv("inputs/members_and_work_addresses.csv")
df_work_addresses.columns = df_work_addresses.columns.str.strip()

#check fields are all there and in order. duplicate names are given ".1" appendage by pandas currently 1-14-26
#expected_order = ["Local Code", "Short #", "Employee No.", "First Name", "Middle Name", "Last Name", 
#                   "Member Active", "Member Status", "Status Date", "Birth Date", "Gender", "Address Line 1", 
#                   "Address Line 2", "City", "State Abbr.", "Zip Code", "Home Phone", "Cell Phone", 
#                   "Work Phone", "Home Email", "Work Email", "AFSCME ID", "Member Type", "Type Date", 
#                   "Card Signature Date", "Agency Code", "Agency Name", "Work Site Work Site Code", 
#                   "Work Site Name", "Building Code Code", "Building Code Name", "Job Class Code", 
#                   "Job Class Name", "Address Type",	"Address Line 1.1" ,"Address Line 2.1", "Address Line 3", 
#                   "City.1", "County", "Job Work City Name", "Work County Name", "Seniority Date", 
#                   "Policy Group Code",	"Policy Group Name", "Bargaining Unit Code", "Bargaining Unit Name", 
#                   "Field Office Name", "Job ID"]

#assert list(df_work_addresses.columns) == expected_order, "Unexpected column order check exports from Unionware for updates."


# concat address line 1 and line 2 with a space between. We may drop this because it looks like Michael was.
#df_work_addresses["Work Address"] = df_work_addresses["Address Line 1.1"].str.cat(df_work_addresses["Address Line 2.1"], sep=" ", na_rep="")
#df_work_addresses["Work Address"] = df_work_addresses["Address Line 1.1"]
df_work_addresses["Work Address"] = df_work_addresses["Address Line 1"]

df_work_addresses["Employee No."] = (
    pd.to_numeric(df_work_addresses["Employee No."], errors="coerce")
      .astype("Int64")
)

work_addresses_to_keep = [
    "Policy Group Code",
    "Work Address",
    "Job Work City Name",
    "Work County Name",
    "Work Site Work Site Code",
    "Field Office Name",
    "Job ID"
]

df_work_addresses = df_work_addresses[work_addresses_to_keep].drop_duplicates()

df_work_addresses.loc[df_work_addresses["Job ID"] == 78674]
#df_work_addresses.head()


# In[ ]:


#members and phones and emails prep

# load data to df
df_phones_and_emails = pd.read_csv("inputs/members_and_phones_emails.csv")

# empty emails or phones if allowed = FALSE
df_phones_and_emails.loc[df_phones_and_emails["Email Allowed"] == "FALSE", "Home Email"] = ""
df_phones_and_emails.loc[df_phones_and_emails["Email Allowed"] == "FALSE", "Work Email"] = ""
df_phones_and_emails.loc[df_phones_and_emails["Email Allowed"] == "FALSE", "External Email"] = ""
df_phones_and_emails.loc[df_phones_and_emails["Phone Allowed"] == "FALSE", "Cell Phone"] = ""

# define fields to keep
#personal
personal_to_keep = [
    "Short #",
    "First Name",
    "Last Name",
    "Home Email",
    "External Email",
    "Cell Phone",
    "Job ID"
]
df_personal = df_phones_and_emails[personal_to_keep]
#work
work_to_keep = [
    "Short #",
    "First Name",
    "Last Name",
    "Work Email",
    "Cell Phone",
    "Job ID"
]
df_work = df_phones_and_emails[work_to_keep]
df_work = df_work.dropna(subset="Work Email")
df_work["Work Email"] = df_work["Work Email"].str.lower()
df_work = df_work.drop_duplicates("Work Email")

# melt/pivot emails into 1 column and drop dupes
df_personal = df_personal.melt(
    id_vars = [
        "Short #",
        "First Name",
        "Last Name",
        "Job ID",
        "Cell Phone"
    ],
    value_vars = [
        "Home Email",
        "External Email"
    ],
    value_name = "Email").dropna(subset=["Email"]).drop(columns=["variable"])
df_personal["Email"] = df_personal["Email"].str.lower()
df_personal = df_personal.drop_duplicates("Email")

df_personal.info()
df_personal.loc[df_personal["Job ID"] == 78674]
#df_work.head()


# In[ ]:


#create df for addresses table
df_addresses = pd.read_csv("inputs/members_and_addresses.csv")

#create mask for filtering on values for mail allowed and bad address
mask = (
    df_addresses["Mail Allowed"].str.strip().str.lower().eq("FALSE") |
    df_addresses["Bad Address"].str.strip().str.lower().eq("TRUE")
)
#apply mask
df_addresses.loc[mask, [
    "Address Line 1",
    "City",
    "State",
    "Zip Code"
    ]
] = ""

#clear dupes to most recent by 'short #'
df_addresses = df_addresses.sort_values(by="Last Updated On").drop_duplicates("Short #", keep='last')

#filter down to only necesarry fields to join
addresses_to_keep = ["Short #","Address Line 1", "City", "State Abbr.", "Zip Code"]
df_addresses = df_addresses[addresses_to_keep]

#check progress
df_addresses.info()


# In[ ]:


# join function
def join_tables (df, personal):
    """
    function to join all of the tables after cleaning

    Parameters:
    - df: input pandas df that will be joined to
    - personal: Boolean to say whether this is to process personal or work emails. True:personal, False:work

    behavior:
    - checks if personal equals True and joins tables
    - drops no longer needed columns
    - returns processed df
    """
    if personal == True:

        # personal list
        df = (
            df
            .merge(df_people, how="left", on="Short #")
            .merge(df_positions, how="left", on="Short #")
            .merge(df_personal_jobs, how="left", on="Email")
            .merge(df_addresses, how="left", on="Short #")
            #.merge(df_contracts, how="left", on="Short #")
            .merge(df_work_addresses, how="left", on="Job ID")
        )
    else:
        #work list
        df = (
            df
            .merge(df_people, how="left", on="Short #")
            .merge(df_positions, how="left", on="Short #")
            .merge(df_work_jobs, how="left", on="Work Email")
            .merge(df_addresses, how="left", on="Short #")
            #.merge(df_contracts, how="left", on="Short #")
            .merge(df_work_addresses, how="left", on="Job ID")
        )

    df = df.drop(columns=["Employee No.","Job ID","AFSCME ID"])

    return df
        #check
    #df_all_personal.loc[df_all_personal["Short #"] == 98349]
        #df_all_personal.info()
        #df_all_work.loc[df_all_personal["Short #"] == 84649]
        #df_all_work.info()


# In[ ]:


df_all_work = join_tables(df_work, False)
df_all_work.info()


# In[ ]:


df_all_personal = join_tables(df_personal, True)
df_all_personal.info()


# In[ ]:


def rename_with_schema(df, personal, schema_csv_path):
    """
    renames headers of a dataframe based on a csv map.

    Parameters:
    - df: a pandas dataframe
    - personal: boolean to determine if processing personal or work emails
    - schema_csv_path: file path to the csv map

    Behavior:
    - loads schema file as variable object
    - creates expected list of headers and actual list of headers to compare
    - raises error if headers are missing
    - renames headers if all expected are present
    """
    schema = pd.read_csv(schema_csv_path)

    actual_cols = set(df.columns)

    if personal == True:

        expected_from = set(schema["from_personal"])

        missing = expected_from - actual_cols

        if missing:
            raise ValueError(f"Missing expected columns: {missing}")
        rename_map = dict(zip(schema["from_personal"], schema["to_personal"]))

        df = df.rename(columns=rename_map)

        return df
    else:
        expected_from = set(schema["from_work"])

        missing = expected_from - actual_cols

        if missing:
            raise ValueError(f"Missing expected columns: {missing}")
        rename_map = dict(zip(schema["from_work"], schema["to_work"]))

        df = df.rename(columns=rename_map)
        return df


# In[ ]:


#normalize values. This can be refactored. will probably want to fill all NaN.
def final_clean(df, personal):
    # 1. Standardize Names & Fill NaNs
    df["First Name"] = df["First Name"].str.lower().str.capitalize().fillna("")
    df["Last Name"] = df["Last Name"].str.lower().str.capitalize().fillna("")

    # 2. Clean Phone Numbers
    phone_cols = ["Cell Phone"]
    df = clean_phone_numbers(df, phone_cols)

    # 3. Map bargaining unit codes
    df = rename_with_schema(df, personal, "helper_tables/final_header_map.csv")

    # 4. Fill blanks in Position related fields with "N"
    position_fields = [
        "c28_local_president",
        "c28_council_eboard",
        "c28_policy_comm_delegate",
        "c28_steward",
        "c28_local_officer",
        "c28_people",
    ]
    for field in position_fields:
        if field in df.columns:
            df[field] = df[field].fillna("N")

    # 5. Handle Dates Safely (Bypasses the "Out of Bounds" type casting crash)
    # We convert to datetime with coerce, then immediately format to string.
    # This keeps the row intact and puts a blank string "" where out-of-bounds dates were.
    for date_col in ["Birth Date", "c28_membership_type_date"]:
        if date_col in df.columns:
            dt_series = pd.to_datetime(df[date_col], errors="coerce", format= "mixed")
            df[date_col] = dt_series.dt.strftime("%m-%d-%Y").fillna("")

    # 6. Enforce remaining non-date Schema safely
    # REMOVED the date columns from here so they don't trigger strict .astype() crashes
    schema = {
        "Unionware_id": "Int64",
        "CellPhone": "Int64",
        "c28_local": "Int64",
        "c28_employer": "Int64",
        "c28_job_classification": "string",
        "c28_bargaining_unit": "string",
        "Zip code": "string",
        "c28_policy_group": "Int64",
    }

    # Only apply schema types to columns that actually exist in the dataframe
    existing_schema = {k: v for k, v in schema.items() if k in df.columns}
    df = df.astype(existing_schema)

    return df


# In[ ]:


df_all_work = final_clean(df_all_work, False)
df_all_personal = final_clean(df_all_personal, True)
df_all_work.head()


# In[ ]:


df_all_work = df_all_work.merge(df_all_personal[["Unionware_id", "Email"]],how="left", on="Unionware_id")
df_all_personal = df_all_personal.merge(df_all_work[["Unionware_id", "Work_email"]], how="left", on="Unionware_id")


# In[ ]:


df_all_work.to_csv("outputs/test_work.csv", index=False)
df_all_personal.to_csv("outputs/test_personal.csv", index=False)


# In[ ]:


df_test = df_all_personal.sample(100)
df_test.to_csv("outputs/test_rows.csv", index=False)


# In[ ]:




