# job_categorizer.py
import re
from collections import defaultdict

def tokenize_ngrams(text):
    text = text.lower()
    tokens = re.findall(r'\w+', text)
    bigrams = [' '.join(pair) for pair in zip(tokens, tokens[1:])]
    return tokens + bigrams

# Manual dictionary for exact title/role matches
manual_role_lookup = {
    "UI/UX": ["ui", "ux", "designer", "ui/ux", "user experience", "user interface"],
    "Database": ["dba", "database", "datawarehousing", "data warehouse"],
    "Mobile Development": ["android", "ios", "mobile developer", "mobile engineer"],
    "Technical Support": ["tech support", "support engineer", "technical support"],
    "Architecture": ["architect", "solution architect", "technical architect"],
    "Data & Analytics": ["data analyst", "data scientist", "business intelligence"],
    "Network": ["network administrator", "network engineer", "system admin"],
    "Cybersecurity": ["security analyst", "security engineer", "system security"],
    "Software/Web development": ["developer", "programmer", "software", "web", "front-end", "backend"],
    "Project & Product Management": ["project manager", "product manager", "program manager", "scrum master"],
    "Quality Assurance & Testing": ["qa", "qc", "tester", "sdet"],
    "Infra & System": ["systems engineer", "system integrator", "infrastructure"],
    "SAP/ERP/CRM": ["sap", "erp", "crm", "abap", "fico", "functional consultant"],
    "Devops & Devops": ["devops", "ci/cd", "automation","aws", "azure", "cloud", "gcp"],
    "It consulting & Solutions": ["consultant", "solutions", "solution architect", "presales"],
    "AI / Data science": ["ai", "artificial intelligence", "machine learning", "ml", "nlp", "computer vision"]
}

# Lowercase and flatten dictionary
manual_lookup_flat = {}
for category, keywords in manual_role_lookup.items():
    for keyword in keywords:
        manual_lookup_flat[keyword.lower()] = category

def match_manual_lookup(text):
    if not text:
        return None
    text = text.lower()
    for keyword, category in manual_lookup_flat.items():
        if keyword in text:
            return category
    return None

# Score-based fallback
def categorize_title_score(title, threshold=1.5):
    tokens = tokenize_ngrams(title)
    scores = defaultdict(float)

    for category, keywords in categories.items():
        for token in tokens:
            scores[category] += keywords.get(token, 0)

    best_category = max(scores, key=scores.get, default=None)
    return best_category if scores[best_category] >= threshold else "Other"

# Weight-based category dictionary for fallback
categories = {
    "Data & Analytics": {
        "data": 2, "analytics": 2, "analyst": 2, "bi": 1.5, "sql": 1.5,
        "scientist": 2, "reporting": 1.5, "dashboard": 1.5, "insights": 1.5
    },
    "Network": {
        "network": 2, "vpn": 1.5, "cisco": 2, "lan": 1.5, "wan": 1.5, "routing": 1.5
    },
    "Cybersecurity": {
        "security": 2, "cybersecurity": 2, "soc": 1.5, "infosec": 1.5, "pentest": 1.5
    },
    "Infra & System": {
        "system": 1.5, "infrastructure": 2, "sysadmin": 2, "administrator": 1.5, "vmware": 1.5
    },
    "Cloud & Devops": {
        "cloud": 2, "aws": 2, "azure": 2, "gcp": 2, "devops": 2,
        "terraform": 2, "ansible": 1.5, "ci/cd": 2, "kubernetes": 1.5, "docker": 1.5
    },
    "Software/Web Development": {
        "developer": 2, "software": 2, "frontend": 1.5, "backend": 1.5,
        "fullstack": 1.5, "programmer": 2, "web": 2, "engineer": 1.5
    },
    "Quality Assurance & Testing": {
        "qa": 2, "testing": 2, "sdet": 2, "tester": 2, "quality": 1.5
    },
    "Project & Product Management": {
        "project manager": 2, "product manager": 2, "scrum": 1.5,
        "agile": 1.5, "program manager": 2, "pmo": 1.5
    },
    "SAP/ERP/CRM": {
        "sap": 2, "erp": 2, "crm": 2, "abap": 2,
        "fico": 2, "sd": 2, "mm": 1.5, "functional consultant": 2,
        "technical consultant": 2
    },
    "AI / Data Science": {
        "ai": 2, "artificial intelligence": 2, "ml": 1.5, "machine learning": 2,
        "deep learning": 1.5, "nlp": 1.5, "computer vision": 1.5
    },
    "IT Consulting & Solutions": {
        "consultant": 2, "solutions": 1.5, "solution architect": 2,
        "presales": 1.5, "implementation": 1.5
    },
    "UI/UX": {
        "ui": 2, "ux": 2, "ui/ux": 2, "user experience": 2, "user interface": 2,
        "interface": 1.5, "interaction design": 1.5, "visual design": 1.5,
        "product design": 1.5, "ux researcher": 1.5
    },
    "Database": {
        "dba": 2, "database": 2, "sql": 1.5, "oracle": 1.5,
        "mysql": 1.5, "postgresql": 1.5, "mongodb": 1.5,
        "datawarehousing": 2, "data warehouse": 2
    },
    "Mobile Development": {
        "android": 2, "ios": 2, "mobile": 2, "mobile developer": 2,
        "flutter": 1.5, "react native": 1.5, "kotlin": 1.5, "swift": 1.5
    },
    "Technical Support": {
        "tech support": 2, "technical support": 2, "support engineer": 2,
        "helpdesk": 1.5, "it support": 1.5, "desktop support": 1.5
    },
    "Architecture": {
        "architect": 2, "solution architect": 2, "technical architect": 2,
        "enterprise architect": 2, "architecture": 1.5
    }
}

# Practical Hybrid Flow
def categorize_job_role(title_or_role):
    if not isinstance(title_or_role, str):
        return "Other"

    # Try manual match first
    manual_result = match_manual_lookup(title_or_role)
    if manual_result:
        return manual_result

    # Fallback to score method
    return categorize_title_score(title_or_role)
