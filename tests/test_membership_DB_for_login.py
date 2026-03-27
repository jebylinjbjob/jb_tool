"""
membership_DB_for_login 模組單元測試
測試登入次數查詢和報告產生功能
"""

import os
import csv
import pytest
from datetime import datetime, date
from unittest.mock import Mock, MagicMock, patch, mock_open
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from membership_DB_for_login import (
    AbpSecurityLogs,
    get_db_engine,
    query_weekly_login_count,
    query_total_login_count,
    generate_csv_report
)


class TestAbpSecurityLogsModel:
    """測試 AbpSecurityLogs 模型"""

    def test_model_tablename(self):
        """測試資料表名稱"""
        assert AbpSecurityLogs.__tablename__ == 'AbpSecurityLogs'

    def test_model_schema(self):
        """測試資料表 schema"""
        assert AbpSecurityLogs.__table_args__ == {'schema': 'dbo'}

    def test_model_has_required_columns(self):
        """測試模型包含必要的欄位"""
        columns = [col.name for col in AbpSecurityLogs.__table__.columns]
        assert 'Id' in columns
        assert 'ApplicationName' in columns
        assert 'CreationTime' in columns


class TestGetDbEngine:
    """測試 get_db_engine 函式"""

    @patch.dict(os.environ, {
        'DB_SERVER': 'test_server',
        'DB_DATABASE': 'test_db',
        'DB_USER_ID': 'test_user',
        'DB_PASSWORD': 'test_password',
        'DB_DRIVER': 'ODBC Driver 17 for SQL Server'
    })
    @patch('membership_DB_for_login.create_engine')
    def test_get_db_engine_success(self, mock_create_engine):
        """測試成功建立資料庫引擎"""
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        result = get_db_engine()

        assert result == mock_engine
        mock_create_engine.assert_called_once()

        # 驗證連接字串包含正確的資訊
        call_args = mock_create_engine.call_args
        connection_string = call_args[0][0]
        assert 'test_user' in connection_string
        assert 'test_password' in connection_string
        assert 'test_server' in connection_string
        assert 'test_db' in connection_string

    @patch.dict(os.environ, {
        'DB_SERVER': 'test_server',
        'DB_DATABASE': 'test_db',
        'DB_USER_ID': '',
        'DB_PASSWORD': 'test_password'
    })
    def test_get_db_engine_missing_username(self):
        """測試缺少使用者名稱"""
        result = get_db_engine()
        assert result is None

    @patch.dict(os.environ, {
        'DB_SERVER': 'test_server',
        'DB_DATABASE': 'test_db',
        'DB_USER_ID': 'test_user',
        'DB_PASSWORD': ''
    })
    def test_get_db_engine_missing_password(self):
        """測試缺少密碼"""
        result = get_db_engine()
        assert result is None

    @patch.dict(os.environ, {
        'DB_SERVER': 'test_server',
        'DB_DATABASE': 'test_db',
        'DB_USER_ID': 'test_user',
        'DB_PASSWORD': 'test_password'
    })
    @patch('membership_DB_for_login.create_engine')
    def test_get_db_engine_default_driver(self, mock_create_engine):
        """測試使用預設驅動程式"""
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        result = get_db_engine()

        assert result == mock_engine
        call_args = mock_create_engine.call_args
        connection_string = call_args[0][0]
        assert 'ODBC+Driver+17+for+SQL+Server' in connection_string

    @patch.dict(os.environ, {
        'DB_SERVER': 'test_server',
        'DB_DATABASE': 'test_db',
        'DB_USER_ID': 'test_user',
        'DB_PASSWORD': 'test_password'
    })
    @patch('membership_DB_for_login.create_engine')
    def test_get_db_engine_exception(self, mock_create_engine):
        """測試建立引擎時發生例外"""
        mock_create_engine.side_effect = Exception("Connection failed")

        result = get_db_engine()
        assert result is None


class TestQueryWeeklyLoginCount:
    """測試 query_weekly_login_count 函式"""

    def test_query_weekly_login_count_success(self):
        """測試成功查詢週登入次數"""
        # 建立 mock engine 和 session
        mock_engine = Mock()
        mock_session = MagicMock(spec=Session)
        mock_query = Mock()
        mock_filter = Mock()

        # 設定 mock 行為
        mock_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_filter
        mock_filter.scalar.return_value = 42

        with patch('membership_DB_for_login.sessionmaker') as mock_sessionmaker:
            mock_sessionmaker.return_value = lambda: mock_session

            result = query_weekly_login_count(
                mock_engine,
                date(2025, 11, 17),
                date(2025, 11, 23)
            )

            assert result == 42
            mock_session.close.assert_called_once()

    def test_query_weekly_login_count_zero_result(self):
        """測試查詢結果為 0"""
        mock_engine = Mock()
        mock_session = MagicMock(spec=Session)
        mock_query = Mock()
        mock_filter = Mock()

        mock_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_filter
        mock_filter.scalar.return_value = 0

        with patch('membership_DB_for_login.sessionmaker') as mock_sessionmaker:
            mock_sessionmaker.return_value = lambda: mock_session

            result = query_weekly_login_count(
                mock_engine,
                date(2025, 11, 17),
                date(2025, 11, 23)
            )

            assert result == 0

    def test_query_weekly_login_count_none_result(self):
        """測試查詢結果為 None 時返回 0"""
        mock_engine = Mock()
        mock_session = MagicMock(spec=Session)
        mock_query = Mock()
        mock_filter = Mock()

        mock_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_filter
        mock_filter.scalar.return_value = None

        with patch('membership_DB_for_login.sessionmaker') as mock_sessionmaker:
            mock_sessionmaker.return_value = lambda: mock_session

            result = query_weekly_login_count(
                mock_engine,
                date(2025, 11, 17),
                date(2025, 11, 23)
            )

            assert result == 0

    def test_query_weekly_login_count_exception(self):
        """測試查詢時發生例外"""
        mock_engine = Mock()
        mock_session = MagicMock(spec=Session)

        mock_session.query.side_effect = Exception("Database error")

        with patch('membership_DB_for_login.sessionmaker') as mock_sessionmaker:
            mock_sessionmaker.return_value = lambda: mock_session

            result = query_weekly_login_count(
                mock_engine,
                date(2025, 11, 17),
                date(2025, 11, 23)
            )

            assert result is None
            mock_session.close.assert_called_once()

    def test_query_weekly_login_count_date_conversion(self):
        """測試日期轉換為 datetime"""
        mock_engine = Mock()
        mock_session = MagicMock(spec=Session)
        mock_query = Mock()
        mock_filter = Mock()

        mock_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_filter
        mock_filter.scalar.return_value = 10

        with patch('membership_DB_for_login.sessionmaker') as mock_sessionmaker:
            mock_sessionmaker.return_value = lambda: mock_session

            start_date = date(2025, 11, 17)
            end_date = date(2025, 11, 23)

            result = query_weekly_login_count(mock_engine, start_date, end_date)

            assert result == 10
            # 驗證 session.query 被呼叫
            mock_session.query.assert_called_once()


class TestQueryTotalLoginCount:
    """測試 query_total_login_count 函式"""

    @patch('membership_DB_for_login.get_total_date_range')
    def test_query_total_login_count_success(self, mock_get_total_date_range):
        """測試成功查詢總登入次數"""
        mock_get_total_date_range.return_value = (date(2025, 11, 17), date(2026, 1, 11))

        mock_engine = Mock()
        mock_session = MagicMock(spec=Session)
        mock_query = Mock()
        mock_filter = Mock()

        mock_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_filter
        mock_filter.scalar.return_value = 1234

        with patch('membership_DB_for_login.sessionmaker') as mock_sessionmaker:
            mock_sessionmaker.return_value = lambda: mock_session

            result = query_total_login_count(mock_engine)

            assert result == 1234
            mock_session.close.assert_called_once()

    @patch('membership_DB_for_login.get_total_date_range')
    def test_query_total_login_count_zero(self, mock_get_total_date_range):
        """測試總登入次數為 0"""
        mock_get_total_date_range.return_value = (date(2025, 11, 17), date(2026, 1, 11))

        mock_engine = Mock()
        mock_session = MagicMock(spec=Session)
        mock_query = Mock()
        mock_filter = Mock()

        mock_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_filter
        mock_filter.scalar.return_value = 0

        with patch('membership_DB_for_login.sessionmaker') as mock_sessionmaker:
            mock_sessionmaker.return_value = lambda: mock_session

            result = query_total_login_count(mock_engine)

            assert result == 0

    @patch('membership_DB_for_login.get_total_date_range')
    def test_query_total_login_count_exception(self, mock_get_total_date_range):
        """測試查詢時發生例外"""
        mock_get_total_date_range.return_value = (date(2025, 11, 17), date(2026, 1, 11))

        mock_engine = Mock()
        mock_session = MagicMock(spec=Session)

        mock_session.query.side_effect = Exception("Database error")

        with patch('membership_DB_for_login.sessionmaker') as mock_sessionmaker:
            mock_sessionmaker.return_value = lambda: mock_session

            result = query_total_login_count(mock_engine)

            assert result is None


class TestGenerateCsvReport:
    """測試 generate_csv_report 函式"""

    def test_generate_csv_report_success(self, tmp_path):
        """測試成功產生 CSV 報告"""
        output_file = tmp_path / "test_report.csv"

        week_counts = [
            {'period': '2025/11月（第3週 11/17~11/23）', 'count': 100},
            {'period': '2025/11月（第4週 11/24~11/30）', 'count': 150},
            {'period': '2025/12月（第1週 12/1~12/7）', 'count': 200}
        ]
        total_count = 450

        generate_csv_report(week_counts, total_count, str(output_file))

        # 驗證檔案存在
        assert output_file.exists()

        # 驗證 CSV 內容
        with open(output_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            rows = list(reader)

            # 驗證標題列
            assert rows[0] == ['期間', '登入次數']

            # 驗證總登入次數
            assert rows[1] == ['總登入人數（2025/11/17~2026/12/31）', '450']

            # 驗證各週統計
            assert rows[2] == ['2025/11月（第3週 11/17~11/23）', '100']
            assert rows[3] == ['2025/11月（第4週 11/24~11/30）', '150']
            assert rows[4] == ['2025/12月（第1週 12/1~12/7）', '200']

    def test_generate_csv_report_empty_weeks(self, tmp_path):
        """測試空的週統計資料"""
        output_file = tmp_path / "test_report_empty.csv"

        week_counts = []
        total_count = 0

        generate_csv_report(week_counts, total_count, str(output_file))

        assert output_file.exists()

        with open(output_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            rows = list(reader)

            assert rows[0] == ['期間', '登入次數']
            assert rows[1] == ['總登入人數（2025/11/17~2026/12/31）', '0']
            assert len(rows) == 2

    def test_generate_csv_report_large_numbers(self, tmp_path):
        """測試大數字"""
        output_file = tmp_path / "test_report_large.csv"

        week_counts = [
            {'period': '2025/11月（第3週 11/17~11/23）', 'count': 999999}
        ]
        total_count = 999999

        generate_csv_report(week_counts, total_count, str(output_file))

        assert output_file.exists()

        with open(output_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            rows = list(reader)

            assert rows[1] == ['總登入人數（2025/11/17~2026/12/31）', '999999']
            assert rows[2] == ['2025/11月（第3週 11/17~11/23）', '999999']

    def test_generate_csv_report_utf8_encoding(self, tmp_path):
        """測試 UTF-8 BOM 編碼"""
        output_file = tmp_path / "test_report_encoding.csv"

        week_counts = [
            {'period': '測試週期', 'count': 100}
        ]
        total_count = 100

        generate_csv_report(week_counts, total_count, str(output_file))

        # 讀取檔案並驗證編碼
        with open(output_file, 'r', encoding='utf-8-sig') as f:
            content = f.read()
            assert '測試週期' in content

    @patch('builtins.open', side_effect=IOError("Permission denied"))
    @patch('membership_DB_for_login.logger')
    def test_generate_csv_report_exception(self, mock_logger, mock_open):
        """測試寫入檔案時發生例外"""
        week_counts = [
            {'period': '2025/11月（第3週 11/17~11/23）', 'count': 100}
        ]
        total_count = 100

        generate_csv_report(week_counts, total_count, "test_report.csv")

        # 驗證錯誤被記錄
        assert mock_logger.error.called

    def test_generate_csv_report_special_characters(self, tmp_path):
        """測試特殊字元處理"""
        output_file = tmp_path / "test_report_special.csv"

        week_counts = [
            {'period': '週期（包含括號）～波浪號', 'count': 100}
        ]
        total_count = 100

        generate_csv_report(week_counts, total_count, str(output_file))

        assert output_file.exists()

        with open(output_file, 'r', encoding='utf-8-sig') as f:
            content = f.read()
            assert '週期（包含括號）～波浪號' in content


class TestIntegration:
    """整合測試"""

    def test_week_counts_structure(self):
        """測試週統計資料結構"""
        week_counts = [
            {'period': '2025/11月（第3週 11/17~11/23）', 'count': 100}
        ]

        # 驗證資料結構
        assert isinstance(week_counts, list)
        assert len(week_counts) > 0
        assert 'period' in week_counts[0]
        assert 'count' in week_counts[0]
        assert isinstance(week_counts[0]['period'], str)
        assert isinstance(week_counts[0]['count'], int)

    def test_csv_report_complete_workflow(self, tmp_path):
        """測試完整的 CSV 報告工作流程"""
        output_file = tmp_path / "complete_report.csv"

        # 模擬完整的週統計資料
        week_counts = [
            {'period': '2025/11月（第3週 11/17~11/23）', 'count': 120},
            {'period': '2025/11月（第4週 11/24~11/30）', 'count': 135},
            {'period': '2025/12月（第1週 12/1~12/7）', 'count': 98},
            {'period': '2025/12月（第2週 12/8~12/14）', 'count': 156},
            {'period': '2025/12月（第3週 12/15~12/21）', 'count': 142},
            {'period': '2025/12月（第4週 12/22~12/28）', 'count': 178},
            {'period': '2026/01月（第1週 12/29~1/4）', 'count': 165},
            {'period': '2026/01月（第2週 1/5~1/11）', 'count': 189}
        ]

        # 計算總數
        total_count = sum(week['count'] for week in week_counts)

        # 產生報告
        generate_csv_report(week_counts, total_count, str(output_file))

        # 驗證檔案
        assert output_file.exists()

        # 驗證內容
        with open(output_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            rows = list(reader)

            # 驗證總行數（標題 + 總計 + 8 週）
            assert len(rows) == 10

            # 驗證總數正確
            assert rows[1][1] == str(total_count)
            assert total_count == 1183
