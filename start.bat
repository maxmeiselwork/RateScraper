@echo off
echo ============================================
echo   Rate Deck Automation Tool
echo ============================================
echo.
echo Installing dependencies (first run only)...
pip install -r requirements.txt --quiet
echo.
echo Starting server...
echo Open http://localhost:5000 in your browser
echo Press Ctrl+C to stop.
echo.
python app.py
pause
