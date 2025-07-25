import json
import pandas as pd
from loguru import logger

def flatten_json_file(json_path):
    with open(json_path, 'r') as f:
        data = json.load(f)

    company = data.get("company")
    location = data.get("location")
    departments = data.get("departments", {})

    rows = []
    for dept_name, dept_info in departments.items():
        for emp_id, emp in dept_info.get("employees", {}).items():
            row = {
                "company": company,
                "location": location,
                "department": dept_name,
                "employee_id": emp_id,
                "name": emp.get("name"),
                "role": emp.get("role"),
                "skills": ", ".join(emp.get("skills", [])),
                "campaigns": ""
            }

            if "projects" in emp:
                row["campaigns"] = ", ".join([
                    f"{p['name']}: {p['status']}" for p in emp["projects"]
                ])
            elif "campaigns" in emp:
                row["campaigns"] = ", ".join([
                    f"{k}: {v}" for k, v in emp["campaigns"].items()
                ])

            rows.append(row)

    df = pd.DataFrame(rows)
    logger.info(f"✅ Flattened {len(df)} employee records from JSON")
    return pd.DataFrame(df)
