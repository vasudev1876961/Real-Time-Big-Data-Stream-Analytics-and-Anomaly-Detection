@echo off
setlocal
echo ========================================================
echo Launching Streamlit Live Analytics Dashboard...
echo ========================================================
.\venv\Scripts\streamlit.exe run dashboard\app.py --server.port 8501
