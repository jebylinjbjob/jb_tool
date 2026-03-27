"""
HireMe 註冊人數統計報告
查詢註冊用戶並按週統計，產生 CSV 報告
"""

import os
import re
import csv
from datetime import datetime, date, timedelta
from typing import Optional, Dict, List, Tuple
from sqlalchemy import create_engine, Column, String, DateTime, Integer, func, and_, or_, select, Table as SQLTable
from sqlalchemy.orm import declarative_base, sessionmaker, aliased
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
import logging
from week_range import get_week_ranges, get_total_date_range

# 載入 .env 檔案
load_dotenv()

# 設定 logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("hireme_report")

# 建立 Base 類別
Base = declarative_base()

HIREME_DATABASE = os.getenv("DB_DATABASE_HireMePlz", "HireMePlz")


# 定義 User 模型
class User(Base):
    """User 資料表模型"""
    __tablename__ = 'User'
    __table_args__ = {'schema': 'dbo'}

    Id = Column(String, primary_key=True)
    LoginName = Column(String)
    lineUid = Column(String)


# 定義 userResumeTempStatus 模型
class UserResumeTempStatus(Base):
    """userResumeTempStatus 資料表模型"""
    __tablename__ = 'userResumeTempStatus'
    __table_args__ = {'schema': 'dbo'}

    userId = Column(String, primary_key=True)
    backId = Column(String)
    hasExport = Column(Integer)
    hasFin = Column(Integer)


# 定義 REC_User 模型（跨資料庫，使用 Table 物件來處理）
from sqlalchemy import Table as SQLTable


def get_db_engine(database: Optional[str] = None) -> Optional[Engine]:
    """
    創建資料庫引擎

    Args:
        database: 資料庫名稱

    Returns:
        資料庫引擎物件，如果失敗則返回 None
    """
    try:
        # 從環境變數讀取資料庫連接資訊
        database_name = database or HIREME_DATABASE
        db_server = os.getenv("DB_SERVER")
        db_username = os.getenv("DB_USER_ID")
        db_password = os.getenv("DB_PASSWORD")
        db_driver = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")

        if not db_username or not db_password or not db_server:
            logger.error("資料庫連接資訊未設定，請檢查環境變數")
            return None

        # 建立連接字串
        connection_string = (
            f"mssql+pyodbc://{db_username}:{db_password}@{db_server}/{database_name}"
            f"?driver={db_driver.replace(' ', '+')}"
            f"&autocommit=True"
        )

        engine = create_engine(
            connection_string,
            echo=False,
            pool_pre_ping=True
        )

        logger.info(f"成功創建資料庫引擎: {db_server}/{database_name}")
        return engine

    except Exception as e:
        logger.error(f"創建資料庫引擎失敗: {e}", exc_info=True)
        return None


def is_email_format(login_name: str) -> bool:
    """
    檢查 LoginName 是否為 email 格式

    Args:
        login_name: 登入名稱

    Returns:
        如果是 email 格式返回 True
    """
    if not login_name:
        return False

    # 簡單的 email 格式檢查：包含 @ 和 .
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(email_pattern, login_name))




def query_registered_users(engine: Engine) -> List[Dict]:
    """
    查詢註冊用戶資料（使用 ORM）

    Args:
        engine: 資料庫引擎

    Returns:
        用戶資料列表
    """
    try:
        # 建立 Session
        SessionLocal = sessionmaker(bind=engine)
        session = SessionLocal()

        try:
            # 對於跨資料庫查詢，使用原生 SQL 但保持 ORM 風格的結果處理
            from sqlalchemy import text

            # 使用原生 SQL 查詢（因為跨資料庫查詢在 ORM 中較複雜）
            # 但使用 ORM 模型來定義結果結構
            sql_query = text(f"""
                SELECT
                    u.Id,
                    u.LoginName,
                    ru.CreateDate,
                    ru.NameC
                FROM {HIREME_DATABASE}.dbo.[User] u
                LEFT JOIN {HIREME_DATABASE}.dbo.userResumeTempStatus urts ON u.Id = urts.userId
                LEFT JOIN JBHRIS_DISPATCH.dbo.REC_User ru ON ru.UserID = urts.backId
                WHERE ru.CreateDate BETWEEN :start_date AND :end_date
                AND u.lineUid IS NOT NULL
            """)

            # 執行查詢
            start_date, end_date = get_total_date_range()
            result = session.execute(
                sql_query,
                {"start_date": start_date, "end_date": end_date}
            )
            results = result.fetchall()

            users = []
            for row in results:
                login_name = row.LoginName if row.LoginName else ""
                # 只保留 email 格式的 LoginName
                if is_email_format(login_name):
                    users.append({
                        'Id': row.Id,
                        'LoginName': login_name,
                        'CreateDate': row.CreateDate,
                        'NameC': row.NameC if row.NameC else ''
                    })

            logger.info(f"查詢到 {len(users)} 位註冊用戶（email 格式）")
            return users

        finally:
            session.close()

    except Exception as e:
        logger.error(f"查詢註冊用戶失敗: {e}", exc_info=True)
        return []


def query_exported_finished_users(engine: Engine) -> List[Dict]:
    """
    查詢已匯出且已完成的用戶資料（hasExport = 1 and hasFin = 1）

    Args:
        engine: 資料庫引擎

    Returns:
        用戶資料列表
    """
    try:
        # 建立 Session
        SessionLocal = sessionmaker(bind=engine)
        session = SessionLocal()

        try:
            from sqlalchemy import text

            sql_query = text(f"""
                SELECT
                    u.Id,
                    u.LoginName,
                    ru.CreateDate,
                    ru.NameC
                FROM {HIREME_DATABASE}.dbo.[User] u
                LEFT JOIN {HIREME_DATABASE}.dbo.userResumeTempStatus urts ON u.Id = urts.userId
                LEFT JOIN JBHRIS_DISPATCH.dbo.REC_User ru ON ru.UserID = urts.backId
                WHERE ru.CreateDate BETWEEN :start_date AND :end_date
                AND u.lineUid IS NOT NULL
                AND urts.hasExport = 1
                AND urts.hasFin = 1
            """)

            # 執行查詢
            start_date, end_date = get_total_date_range()
            result = session.execute(
                sql_query,
                {"start_date": start_date, "end_date": end_date}
            )
            results = result.fetchall()

            users = []
            for row in results:
                login_name = row.LoginName if row.LoginName else ""
                # 只保留 email 格式的 LoginName
                if is_email_format(login_name):
                    users.append({
                        'Id': row.Id,
                        'LoginName': login_name,
                        'CreateDate': row.CreateDate,
                        'NameC': row.NameC if row.NameC else ''
                    })

            logger.info(f"查詢到 {len(users)} 位已匯出且已完成的用戶（email 格式）")
            return users

        finally:
            session.close()

    except Exception as e:
        logger.error(f"查詢已匯出且已完成的用戶失敗: {e}", exc_info=True)
        return []


def count_users_by_week(users: List[Dict], week_start: date, week_end: date) -> int:
    """
    統計指定週的註冊人數

    Args:
        users: 用戶列表
        week_start: 週開始日期
        week_end: 週結束日期

    Returns:
        該週的註冊人數
    """
    count = 0
    for user in users:
        if user['CreateDate']:
            create_date = user['CreateDate']
            # 如果是 datetime 物件，轉換為 date
            if isinstance(create_date, datetime):
                create_date = create_date.date()
            elif isinstance(create_date, str):
                create_date = datetime.strptime(create_date, "%Y-%m-%d").date()

            if week_start <= create_date <= week_end:
                count += 1

    return count


def generate_csv_report(users: List[Dict], output_file: str = "hireme_registration_report.csv"):
    """
    產生 CSV 報告

    Args:
        users: 用戶列表
        output_file: 輸出檔案名稱
    """
    try:
        # 計算總註冊人數
        total_count = len(users)

        # 取得各週的統計
        weeks = get_week_ranges()
        week_counts = []
        for week_desc, week_start, week_end, week_label in weeks:
            count = count_users_by_week(users, week_start, week_end)
            week_counts.append({
                'period': week_desc,
                'count': count
            })

        # 寫入 CSV
        with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)

            # 寫入標題
            writer.writerow(['期間', '註冊人數'])

            # 寫入總註冊人數
            writer.writerow([f'總註冊人數（11/17~1/11）', total_count])

            # 寫入各週統計
            for week_data in week_counts:
                writer.writerow([week_data['period'], week_data['count']])

        logger.info(f"CSV 報告已產生: {output_file}")

        # 輸出到控制台
        print(f"\n{'='*60}")
        print("HireMe 註冊人數統計報告")
        print(f"{'='*60}")
        print(f"總註冊人數（11/17~1/11）：{total_count:,}")
        for week_data in week_counts:
            print(f"{week_data['period']}：{week_data['count']:,}")
        print(f"{'='*60}\n")
        print(f"報告已儲存至: {output_file}\n")

    except Exception as e:
        logger.error(f"產生 CSV 報告失敗: {e}", exc_info=True)


def generate_exported_finished_csv_report(users: List[Dict], output_file: str = "hireme_exported_finished_report.csv"):
    """
    產生已匯出且已完成的用戶 CSV 報告

    Args:
        users: 用戶列表
        output_file: 輸出檔案名稱
    """
    try:
        # 計算總人數
        total_count = len(users)

        # 取得各週的統計
        weeks = get_week_ranges()
        week_counts = []
        for week_desc, week_start, week_end, week_label in weeks:
            count = count_users_by_week(users, week_start, week_end)
            week_counts.append({
                'period': week_desc,
                'count': count
            })

        # 寫入 CSV
        with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)

            # 寫入標題
            writer.writerow(['期間', '已匯出且已完成人數'])

            # 寫入總人數
            writer.writerow([f'總人數（11/17~1/11）', total_count])

            # 寫入各週統計
            for week_data in week_counts:
                writer.writerow([week_data['period'], week_data['count']])

        logger.info(f"已匯出且已完成用戶 CSV 報告已產生: {output_file}")

        # 輸出到控制台
        print(f"\n{'='*60}")
        print("HireMe 已匯出且已完成用戶統計報告")
        print(f"{'='*60}")
        print(f"總人數（11/17~1/11）：{total_count:,}")
        for week_data in week_counts:
            print(f"{week_data['period']}：{week_data['count']:,}")
        print(f"{'='*60}\n")
        print(f"報告已儲存至: {output_file}\n")

    except Exception as e:
        logger.error(f"產生已匯出且已完成用戶 CSV 報告失敗: {e}", exc_info=True)


def main():
    """
    主函數
    """
    logger.info("開始產生 HireMe 註冊人數統計報告")

    # 創建資料庫引擎
    engine = get_db_engine()
    if not engine:
        logger.error("無法創建資料庫引擎，程式結束")
        return

    try:
        # 查詢註冊用戶
        users = query_registered_users(engine)

        if not users:
            logger.warning("未查詢到任何註冊用戶")
            print("\n未查詢到任何註冊用戶（email 格式）\n")
        else:
            # 產生 CSV 報告
            generate_csv_report(users)

        # 查詢已匯出且已完成的用戶
        exported_finished_users = query_exported_finished_users(engine)

        if not exported_finished_users:
            logger.warning("未查詢到任何已匯出且已完成的用戶")
            print("\n未查詢到任何已匯出且已完成的用戶（email 格式）\n")
        else:
            # 產生已匯出且已完成的用戶 CSV 報告
            generate_exported_finished_csv_report(exported_finished_users)

    finally:
        # 關閉資料庫引擎
        engine.dispose()
        logger.info("資料庫引擎已關閉")


if __name__ == "__main__":
    main()
