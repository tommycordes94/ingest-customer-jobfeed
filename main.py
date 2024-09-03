import os
from flask import Flask, request
import logging
import traceback
from datetime import datetime

from logging.config import dictConfig
from trdpipe.structify_publish.helper import loadConfig

from service.ingest import CompanyJobfeedIngester

dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format': '%(message)s',
    }},
    'handlers': {'console': {
        'class': 'logging.StreamHandler',
        'stream': 'ext://sys.stdout',
        'formatter': 'default'
    }},
    'root': {
        'level': 'INFO',
        'handlers': ['console']
    }
})

app = Flask(__name__)

@app.route('/', methods=['GET'])
def ingest():

    try:
        env = request.args.get("env")
        if env is None:
            raise ValueError("Please specify an environment.")
        
        company = request.args.get("company")
        if company is None:
            raise ValueError("Please specify a company the jobfeed should be ingested for")

        config = loadConfig(env,'')

        i = CompanyJobfeedIngester(config=config, company=company)
        i.ingest()

        return {
            "message":f"jobfeed for {company} sucessfully suggested",
            "date": datetime.now().strftime("%Y%m%d")
        }, 200
    
    except Exception as e:
        error = f"ERROR OCCURED: {e}"
        logging.error(error)
        logging.error(traceback.format_exc())
        return error, 400

if __name__ == "__main__":
    app.run(
        debug=True, 
        host="0.0.0.0", 
        port=int(os.environ.get("PORT", 8080))
    )