from flask import Markup
from flask import Flask
from flask import render_template
from bigquery import get_client

import os
import json
import time


def wait_until(somepredicate, timeout, period=0.25, *args, **kwargs):
    mustend = time.time() + timeout
    while time.time() < mustend:
        if somepredicate(*args, **kwargs):
            return True
        time.sleep(period)
    return False


app = Flask(__name__)


@app.route('/hello')
def hello_world():
    return 'Hello World!'


@app.route('/querytest')
def qtest():
    json_key = os.path.join(app.config['DATA_DIR'], 'My First Project-305866c8ff55.json')
    client = get_client(json_key_file=json_key, readonly=True)

    # Submit an async query.
    job_id, _results = client.query('SELECT * FROM [gdelt-bq:internetarchivebooks.1800]  LIMIT 10')

    # Check if the query has finished running.
    complete, row_count = client.check_job(job_id)

    wait_until(lambda c=client, id=job_id: client.check_job(id), 10000)

    results = client.get_query_rows(job_id)
    return str(results)


@app.route("/chart")
def chart():
    labels = ["January", "February", "March", "April", "May", "June", "July", "August"]
    values = [10, 9, 8, 7, 6, 4, 7, 8]
    return render_template('lineChart.html', values=values, labels=labels)


if __name__ == "__main__":
    app.config['DATA_DIR'] = "/home/alexj/Projects/PycharmProjects/Flask_test/data"
    app.run(host='127.0.0.1', port=8001)


