# gunicorn_config.py
loglevel = 'info'
accesslog = './Api/temp/gunicorn-access.log'
errorlog = './Api/temp/gunicorn-error.log'
access_log_format = '%({X-Forwarded-For}i)s - %(t)s - "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"'