### Script
``` 
  ~/anaconda3/bin/python mac.py
  
  gunicorn -c gunicorn_config.py -k uvicorn.workers.UvicornWorker main:app --bind 0.0.0.0:7901 --workers 16

  chmod +x /Users/checkmate/Work/checkmateStock/utils/mongodb_backup.sh
```
### 2024.01.18
- MiddleWare API