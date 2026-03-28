import re
import sqlite3
import pandas as pd
from pathlib import Path

# ===== 配置 =====
db_path = "hrs_scraper.db"
xlsx_path = "hrs_scraper_export.xlsx"
exclude_tables = {"sqlite_sequence"}
# =================

# Excel 不允许的控制字符
ILLEGAL_CHARACTERS_RE = re.compile(
    r"[\x00-\x08\x0B-\x0C\x0E-\x1F]"
)

def clean_illegal_excel_chars(value):
    """清除 Excel 单元格不允许的非法字符"""
    if isinstance(value, str):
        return ILLEGAL_CHARACTERS_RE.sub("", value)
    return value

def sanitize_sheet_name(name: str) -> str:
    invalid_chars = ['\\', '/', '*', '?', ':', '[', ']']
    for ch in invalid_chars:
        name = name.replace(ch, "_")
    return name[:31]

def clean_dataframe_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """清洗整个 DataFrame，避免 openpyxl 报错"""
    return df.map(clean_illegal_excel_chars)

def export_sqlite_to_xlsx(db_path: str, xlsx_path: str):
    db_path = Path(db_path)
    xlsx_path = Path(xlsx_path)

    if not db_path.exists():
        raise FileNotFoundError(f"数据库文件不存在: {db_path}")

    conn = sqlite3.connect(db_path)

    try:
        tables_query = """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
        ORDER BY name;
        """
        tables = pd.read_sql_query(tables_query, conn)["name"].tolist()
        tables = [t for t in tables if t not in exclude_tables]

        if not tables:
            raise ValueError("数据库中没有可导出的表。")

        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            for table in tables:
                print(f"正在导出表: {table}")
                df = pd.read_sql_query(f'SELECT * FROM "{table}"', conn)

                # 清洗非法字符
                df = clean_dataframe_for_excel(df)

                sheet_name = sanitize_sheet_name(table)
                df.to_excel(writer, sheet_name=sheet_name, index=False)

        print(f"\n导出完成: {xlsx_path.resolve()}")
        print("已导出的表：")
        for t in tables:
            print(f" - {t}")

    finally:
        conn.close()

if __name__ == "__main__":
    export_sqlite_to_xlsx(db_path, xlsx_path)