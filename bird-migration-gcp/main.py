"""Function called by PubSub trigger to execute cron job tasks."""
import datetime
import logging
from string import Template
import function.bird_to_gcp as bird_to_gcp
from google.cloud import bigquery
from flask import Flask, request

app = Flask(__name__)

@app.route('/bird-migration-gcp/bird_to_gcp')
def start_bird_to_gcp(): 
    is_cron = request.headers.get('X-Appengine-Cron', False)
    if not is_cron:
        return 'Bad Request', 400

    try:
        append_data.run() #the actual name of the script/function you want to run contained in the subfolder
        return "Pipeline started", 200
    except Exception as e:
        logging.exception(e)
        return "Error: <pre>{}</pre>".format(e), 500

@app.errorhandler(500) #error handling script for troubleshooting
def server_error(e):
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500

if __name__ == '__main__': #hosting administration syntax
    app.run(host='127.0.0.1', port=8080, debug=True)