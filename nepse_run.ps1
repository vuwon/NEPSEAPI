# NEPSE Run Script — double-click or run from PowerShell
# Automatically sets all credentials and runs compile

$env:SUPABASE_URL      = "https://kthmokgpxkksyytvuvvl.supabase.co"
$env:SUPABASE_KEY      = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imt0aG1va2dweGtrc3l5dHZ1dnZsIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NjQ3Njc3OSwiZXhwIjoyMDkyMDUyNzc5fQ.wa-aTVrlm5WXZ5Trluraj2Ps5-ajKVOdb74DySKDenA"
$env:SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imt0aG1va2dweGtrc3l5dHZ1dnZsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzY0NzY3NzksImV4cCI6MjA5MjA1Mjc3OX0.cXHBwFuzaIvr9h5G8lniyfdngZO_1_b-EwkhIbcBKAg"
$env:TRADES_KEY        = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZtc2VpemN1YmJpZW9kdmZ1dGJ5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3OTA1MjU1OSwiZXhwIjoyMDk0NjI4NTU5fQ.kqsS0SsOto6OC0o9TherbNCQL50EAVMHD8PZaBJBv78"

Set-Location "D:\F\NEPSE\NEPSE-API-Test"

Write-Host "Running compile_holdings.py..." -ForegroundColor Cyan
python compile_holdings.py

Write-Host "Pushing to GitHub..." -ForegroundColor Cyan
git add -f index.html compile_holdings.py
git commit -m "Dashboard update $(Get-Date -Format 'yyyy-MM-dd')"
git push origin main

Write-Host "Done!" -ForegroundColor Green
