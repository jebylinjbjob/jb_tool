"""
查詢前台登入次數（按週統計）
User story: 我想要知道這個月有多少人登入過我的網站
並且讓我知道他的
"""

import os
import csv
from datetime import datetime, date
from typing import Optional, List, Tuple, Dict
from sqlalchemy import create_engine, Column, Integer, String, DateTime, func
from sqlalchemy.orm import declarative_base, sessionmaker, Session
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
logger = logging.getLogger("membership_db")

# 建立 Base 類別
Base = declarative_base()


# 定義 AbpSecurityLogs 模型
class AbpSecurityLogs(Base):
    """
    AbpSecurityLogs 資料表模型
    """
    __tablename__ = 'AbpSecurityLogs'
    __table_args__ = {'schema': 'dbo'}

    # 定義欄位（根據查詢需求，只定義必要的欄位）
    Id = Column(String, primary_key=True)
    ApplicationName = Column(String)
    CreationTime = Column(DateTime)
    # 其他欄位可以根據需要添加


def get_db_engine() -> Optional[Engine]:
    """
    創建資料庫引擎

    Returns:
        資料庫引擎物件，如果失敗則返回 None
    """
    try:
        # 從環境變數讀取資料庫連接資訊
        db_server = os.getenv("DB_SERVER")
        db_database = os.getenv("DB_DATABASE")
        db_username = os.getenv("DB_USER_ID")
        db_password = os.getenv("DB_PASSWORD")
        db_driver = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")

        if not db_username or not db_password:
            logger.error("資料庫使用者名稱或密碼未設定，請檢查環境變數 DB_USERNAME 和 DB_PASSWORD")
            return None

        # 建立連接字串
        connection_string = (
            f"mssql+pyodbc://{db_username}:{db_password}@{db_server}/{db_database}"
            f"?driver={db_driver.replace(' ', '+')}"
            f"&autocommit=True"
        )

        engine = create_engine(
            connection_string,
            echo=False,
            pool_pre_ping=True
        )

        logger.info(f"成功創建資料庫引擎: {db_server}/{db_database}")
        return engine

    except Exception as e:
        logger.error(f"創建資料庫引擎失敗: {e}", exc_info=True)
        return None




def query_weekly_login_count(engine: Engine, week_start: date, week_end: date) -> Optional[int]:
    """
    查詢指定週的前台登入次數（使用 ORM）

    Args:
        engine: 資料庫引擎
        week_start: 週開始日期
        week_end: 週結束日期

    Returns:
        該週的登入次數，如果失敗則返回 None
    """
    try:
        # 轉換為 datetime 物件
        start_datetime = datetime.combine(week_start, datetime.min.time())
        end_datetime = datetime.combine(week_end, datetime.max.time())

        # 建立 Session
        SessionLocal = sessionmaker(bind=engine)
        session = SessionLocal()

        try:
            # 使用 ORM 查詢
            count = (
                session.query(func.count(AbpSecurityLogs.Id))
                .filter(
                    AbpSecurityLogs.ApplicationName == 'Public.JbJobMembership.HttpApi.Host',
                    AbpSecurityLogs.CreationTime >= start_datetime,
                    AbpSecurityLogs.CreationTime <= end_datetime
                )
                .scalar()
            )

            if count is not None:
                return count
            else:
                return 0

        finally:
            session.close()

    except Exception as e:
        logger.error(f"查詢週登入次數失敗: {e}", exc_info=True)
        return None


def query_total_login_count(engine: Engine) -> Optional[int]:
    """
    查詢總登入次數（11/17~1/11）

    Args:
        engine: 資料庫引擎

    Returns:
        總登入次數，如果失敗則返回 None
    """
    try:
        start_date, end_date = get_total_date_range()

        # 轉換為 datetime 物件
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())

        # 建立 Session
        SessionLocal = sessionmaker(bind=engine)
        session = SessionLocal()

        try:
            # 使用 ORM 查詢
            count = (
                session.query(func.count(AbpSecurityLogs.Id))
                .filter(
                    AbpSecurityLogs.ApplicationName == 'Public.JbJobMembership.HttpApi.Host',
                    AbpSecurityLogs.CreationTime >= start_datetime,
                    AbpSecurityLogs.CreationTime <= end_datetime
                )
                .scalar()
            )

            if count is not None:
                return count
            else:
                return 0

        finally:
            session.close()

    except Exception as e:
        logger.error(f"查詢總登入次數失敗: {e}", exc_info=True)
        return None


def generate_csv_report(week_counts: List[Dict], total_count: int, output_file: str = "membership_login_report.csv"):
    """
    產生 CSV 報告

    Args:
        week_counts: 各週統計列表
        total_count: 總登入次數
        output_file: 輸出檔案名稱
    """
    try:
        start_date, end_date = get_total_date_range()
        date_range_label = f"{start_date.strftime('%Y/%m/%d')}~{end_date.strftime('%Y/%m/%d')}"

        # 寫入 CSV
        with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)

            # 寫入標題
            writer.writerow(['期間', '登入次數'])

            # 寫入總登入次數
            writer.writerow([f'總登入人數（{date_range_label}）', total_count])

            # 寫入各週統計
            for week_data in week_counts:
                writer.writerow([week_data['period'], week_data['count']])

        logger.info(f"CSV 報告已產生: {output_file}")

        # 輸出到控制台
        print(f"\n{'='*60}")
        print("前台登入次數統計報告")
        print(f"{'='*60}")
        print(f"總登入人數（{date_range_label}）：{total_count:,}")
        for week_data in week_counts:
            print(f"{week_data['period']}：{week_data['count']:,}")
        print(f"{'='*60}\n")
        print(f"報告已儲存至: {output_file}\n")

    except Exception as e:
        logger.error(f"產生 CSV 報告失敗: {e}", exc_info=True)


def main():
    """
    主函數
    """
    logger.info("開始查詢前台登入次數（按週統計）")

    # 創建資料庫引擎
    engine = get_db_engine()
    if not engine:
        logger.error("無法創建資料庫引擎，程式結束")
        return

    try:
        # 查詢總登入次數
        total_count = query_total_login_count(engine)
        if total_count is None:
            logger.error("查詢總登入次數失敗")
            return

        # 取得各週的統計
        weeks = get_week_ranges()
        week_counts = []

        for week_desc, week_start, week_end, week_label in weeks:
            count = query_weekly_login_count(engine, week_start, week_end)
            if count is not None:
                week_counts.append({
                    'period': week_desc,
                    'count': count
                })
                logger.info(f"{week_desc}: {count} 次")
            else:
                logger.warning(f"查詢 {week_desc} 失敗")

        # 產生 CSV 報告
        generate_csv_report(week_counts, total_count)

    finally:
        # 關閉資料庫引擎
        engine.dispose()
        logger.info("資料庫引擎已關閉")


if __name__ == "__main__":
    main()
