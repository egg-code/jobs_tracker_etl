import pandas as pd
import hashlib

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
    df = df.copy()
    # Create short hash from job_link + source
    def generate_hash(row):
        base = (str(row['job_link']) + str(row['source'])).strip().lower()
        return hashlib.md5(base.encode()).hexdigest()[:6]
    
    df['hash'] = df.apply(generate_hash, axis=1)

    # Map codes
    df['cat_code'] = df['category'].map(category_map).fillna("xx")
    df['src_code'] = df['source'].map(source_map).fillna("xxx")
    df['c_code'] = df['country'].str.lower()
    df['ym'] = pd.to_datetime(df['date_posted']).dt.strftime("%Y%m")
    
    # Final job_id
    df['job_id'] = (
        df['hash'] + "_" + df['cat_code'] + "_" +
        df['src_code'] + "_" + df['c_code'] + "_" + df['ym']
    )

    # Drop helper columns
    df.drop(columns=['hash', 'cat_code', 'src_code', 'c_code', 'ym'], inplace=True)

    return df[['job_id'] + [col for col in df.columns if col != 'job_id']]