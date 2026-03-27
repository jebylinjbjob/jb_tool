# 設定 Windows 使用 PowerShell
# 使用WSL 或是 linux 幫我註解這邊

set shell := ["powershell.exe", "-c"]

# 預設任務:顯示所有可用的指令
default:
    just --fmt --unstable 2> $null
    just --list --unsorted

# 檢查是否已初始化，如果沒有就初始化
init:
    @Write-Host "檢查專案初始化狀態..."
    @if (-not (Test-Path ".venv") -and -not (Test-Path "venv")) { Write-Host "建立虛擬環境..."; if (Get-Command uv -ErrorAction SilentlyContinue) { uv venv } else { python -m venv venv } } else { Write-Host "虛擬環境已存在" }
    @if (-not (Test-Path ".env")) { Write-Host "建立 .env 檔案..."; if (Test-Path ".env.example") { Copy-Item ".env.example" ".env"; Write-Host ".env 檔案已建立，請編輯填入實際的資料庫連接資訊" } else { Write-Host "請手動建立 .env 檔案（參考 .env.example）" } } else { Write-Host ".env 檔案已存在" }
    @Write-Host "安裝依賴套件..."
    @if (Get-Command uv -ErrorAction SilentlyContinue) { uv pip install -r requirements.txt } else { pip install -r requirements.txt }
    @Write-Host "初始化完成！"

# 執行 hireme.py
hireme:
    @Write-Host "執行 HireMe 註冊人數統計..."
    @if (Get-Command uv -ErrorAction SilentlyContinue) { uv run python hireme.py } else { python hireme.py }

# 執行 membership_DB_for_login.py
login:
    @Write-Host "執行前台登入次數統計..."
    @if (Get-Command uv -ErrorAction SilentlyContinue) { uv run python membership_DB_for_login.py } else { python membership_DB_for_login.py }

# 執行 network_speedtest.py
speedtest:
    @Write-Host "執行網速測試..."
    @if (Get-Command uv -ErrorAction SilentlyContinue) { uv run python network_speedtest.py } else { python network_speedtest.py }

# 執行所有報告
all:
    @just hireme
    @just login

# 執行測試
test:
    @Write-Host "執行單元測試..."
    @if (Get-Command uv -ErrorAction SilentlyContinue) { uv run pytest tests/ -v } else { python -m pytest tests/ -v }

# 程式碼檢查
lint:
    @Write-Host "執行程式碼檢查..."
    @if (Get-Command uv -ErrorAction SilentlyContinue) { uv pip install flake8; uv run flake8 . --count --show-source --statistics } else { pip install flake8; flake8 . --count --show-source --statistics }

# 執行測試並顯示覆蓋率
test-cov:
    @Write-Host "執行測試並顯示覆蓋率..."
    @if (Get-Command uv -ErrorAction SilentlyContinue) { uv pip install pytest-cov; uv run pytest tests/ --cov=. --cov-report=term-missing --cov-report=html } else { pip install pytest-cov; python -m pytest tests/ --cov=. --cov-report=term-missing --cov-report=html }
    @Write-Host "覆蓋率報告已產生於 htmlcov/index.html"

# 更新依賴套件
update:
    @Write-Host "更新依賴套件..."
    @if (Get-Command uv -ErrorAction SilentlyContinue) { uv pip install -r requirements.txt --upgrade } else { pip install -r requirements.txt --upgrade }

# 清理虛擬環境
clean:
    @Write-Host "清理虛擬環境..."
    @if (Test-Path ".venv") { Remove-Item -Recurse -Force ".venv"; Write-Host "已刪除 .venv" }
    @if (Test-Path "venv") { Remove-Item -Recurse -Force "venv"; Write-Host "已刪除 venv" }
    @Write-Host "虛擬環境已清理"

# 檢查環境變數設定
check-env:
    @Write-Host "檢查環境變數設定..."
    @if (Test-Path ".env") { Write-Host ".env 檔案存在"; python -c "from dotenv import load_dotenv; import os; load_dotenv(); print('DB_SERVER:', os.getenv('DB_SERVER', '未設定')); print('DB_DATABASE:', os.getenv('DB_DATABASE', '未設定')); print('DB_USER_ID:', os.getenv('DB_USER_ID', '未設定')); print('DB_PASSWORD:', '***' if os.getenv('DB_PASSWORD') else '未設定')" } else { Write-Host "警告: .env 檔案不存在，請先執行 'just init' 或手動建立 .env 檔案" }
