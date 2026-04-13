# Script kiem tra moi truong crawler
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Kiem tra moi truong Crawler" -ForegroundColor Cyan
Write-Host "============================================`n" -ForegroundColor Cyan

$allOk = $true

# 1. Python
Write-Host "--- Kiem tra Python ---" -ForegroundColor Yellow
try {
    $pythonVersion = python --version 2>&1
    Write-Host "[OK] $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "[MISSING] Python khong tim thay" -ForegroundColor Red
    $allOk = $false
}

# 2. Pip
Write-Host "`n--- Kiem tra pip ---" -ForegroundColor Yellow
try {
    $pipVersion = pip --version 2>&1
    Write-Host "[OK] $pipVersion" -ForegroundColor Green
} catch {
    Write-Host "[MISSING] pip khong tim thay" -ForegroundColor Red
    $allOk = $false
}

# 3. Scrapy
Write-Host "`n--- Kiem tra Scrapy ---" -ForegroundColor Yellow
try {
    $scrapyVersion = scrapy version 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[OK] Scrapy da cai dat" -ForegroundColor Green
    } else {
        Write-Host "[MISSING] Scrapy chua cai dat" -ForegroundColor Red
        Write-Host "  -> Chay: pip install scrapy" -ForegroundColor Yellow
        $allOk = $false
    }
} catch {
    Write-Host "[MISSING] Scrapy chua cai dat" -ForegroundColor Red
    Write-Host "  -> Chay: pip install scrapy" -ForegroundColor Yellow
    $allOk = $false
}

# 4. Playwright
Write-Host "`n--- Kiem tra Playwright ---" -ForegroundColor Yellow
try {
    python -c "import playwright; print('Playwright version:', playwright.__version__)" 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[OK] Playwright da cai dat" -ForegroundColor Green
    } else {
        Write-Host "[MISSING] Playwright chua cai dat" -ForegroundColor Red
        Write-Host "  -> Chay: pip install playwright" -ForegroundColor Yellow
        $allOk = $false
    }
} catch {
    Write-Host "[MISSING] Playwright chua cai dat" -ForegroundColor Red
    Write-Host "  -> Chay: pip install playwright" -ForegroundColor Yellow
    $allOk = $false
}

# 5. Playwright browsers
Write-Host "`n--- Kiem tra Playwright browsers ---" -ForegroundColor Yellow
try {
    $playwrightBrowsers = playwright show-browsers 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[OK] Playwright browsers da cai dat" -ForegroundColor Green
    } else {
        Write-Host "[MISSING] Playwright browsers chua cai dat" -ForegroundColor Red
        Write-Host "  -> Chay: playwright install chromium" -ForegroundColor Yellow
        $allOk = $false
    }
} catch {
    Write-Host "[MISSING] Playwright browsers chua cai dat" -ForegroundColor Red
    Write-Host "  -> Chay: playwright install chromium" -ForegroundColor Yellow
    $allOk = $false
}

# 6. Scrapy-playwright
Write-Host "`n--- Kiem tra scrapy-playwright ---" -ForegroundColor Yellow
try {
    python -c "import scrapy_playwright" 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[OK] scrapy-playwright da cai dat" -ForegroundColor Green
    } else {
        Write-Host "[MISSING] scrapy-playwright chua cai dat" -ForegroundColor Red
        Write-Host "  -> Chay: pip install scrapy-playwright" -ForegroundColor Yellow
        $allOk = $false
    }
} catch {
    Write-Host "[MISSING] scrapy-playwright chua cai dat" -ForegroundColor Red
    Write-Host "  -> Chay: pip install scrapy-playwright" -ForegroundColor Yellow
    $allOk = $false
}

# 7. Kiem tra thu muc crawler
Write-Host "`n--- Kiem tra thu muc crawler ---" -ForegroundColor Yellow
if (Test-Path "crawler") {
    Write-Host "[OK] Thu muc crawler ton tai" -ForegroundColor Green
    
    if (Test-Path "crawler/scrapy.cfg") {
        Write-Host "[OK] scrapy.cfg ton tai" -ForegroundColor Green
    } else {
        Write-Host "[MISSING] scrapy.cfg khong ton tai" -ForegroundColor Red
        $allOk = $false
    }
    
    if (Test-Path "crawler/spiders") {
        Write-Host "[OK] Thu muc spiders ton tai" -ForegroundColor Green
    } else {
        Write-Host "[MISSING] Thu muc spiders khong ton tai" -ForegroundColor Red
        $allOk = $false
    }
} else {
    Write-Host "[MISSING] Thu muc crawler khong ton tai" -ForegroundColor Red
    $allOk = $false
}

# Ket luan
Write-Host "`n============================================" -ForegroundColor Cyan
if ($allOk) {
    Write-Host "  Moi truong da san sang!" -ForegroundColor Green
    Write-Host "`n  Buoc tiep theo:" -ForegroundColor Cyan
    Write-Host "  1. cd crawler" -ForegroundColor White
    Write-Host "  2. scrapy crawl thuvienphapluat -O output/test.json" -ForegroundColor White
} else {
    Write-Host "  Can cai dat them mot so package!" -ForegroundColor Red
    Write-Host "`n  Cai dat tat ca:" -ForegroundColor Cyan
    Write-Host "  pip install -r requirements.txt" -ForegroundColor White
    Write-Host "  playwright install chromium" -ForegroundColor White
}
Write-Host "============================================`n" -ForegroundColor Cyan
