#!/bin/bash
exec /home/zhozhinc/.local/bin/gunicorn -c "/home/zhozhinc/code/sites/c_api/gunicorn.conf.py" -k uvicorn.workers.UvicornWorker main:app
