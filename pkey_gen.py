import pandas as pd

## Map dicts
category_map = {
    "Data & Analytics" : "da",
    "Network" : "nw",
    "Cybersecurity" : "cy",
    "Infra & System" : "is",
    "Cloud & Devops" : "cd",
    "Software/Web Development" : "sw",
    "Project & Product Management" : "pm",
    "Quality Assurance & Testing" : "qa",
    "SAP / ERP / CRM" : "sp",
    "IT Consulting & Solutions" : "co",
    "AI / Data Science" : "ai",
    "Sales / Technical Sales" : "sa",
    "UI/UX" : "ui",
    "Database" : "db",
    "Mobile Development" : "md",
    "Technical Support" : "ts",
    "Other" : "ot"
}

source_map = {
    "founditsg" : "fit",
    "jobnetmm" : "jmm",
    "jobsdbsg" : "jsg",
    "jobsdbth" : "jth",
    "jobstreetmalay" : "jst"
}

def custom_job_id(df):
    df = df.sort_values("job_link").copy()
    df.reset_index(drop=True, inplace=True)
    df['uid'] = df.index + 1
    df['uid'] = df['uid'].astype(str).str.zfill(5)
    df['cat_code'] = df['category'].map(category_map).fillna("xx")
    df['src_code'] = df['source'].map(source_map).fillna("xxx")
    df['c_code'] = df['country'].str.lower()
    df['ym'] = pd.to_datetime(df['date_posted']).dt.strftime("%Y%m")
    df['job_id'] = df['uid'] + "_" + df['cat_code'] + "_" + df['src_code'] + "_" + df['c_code'] + "_" + df['ym']
    df.drop(columns=['uid', 'cat_code', 'src_code', 'c_code', 'ym'], inplace=True)

    return df[['job_id'] + [col for col in df.columns if col != 'job_id']]