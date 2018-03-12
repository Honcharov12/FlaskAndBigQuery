from flask import Markup
from flask import Flask
from flask import render_template
from bigquery import get_client

import os
import json
import time
import ast


def wait_until(somepredicate, timeout, period=0.25, *args, **kwargs):
    mustend = time.time() + timeout
    while time.time() < mustend:
        if somepredicate(*args, **kwargs):
            return True
        time.sleep(period)
    return False


app = Flask(__name__)


def qtest():
    json_key = os.path.join(os.getcwd() + "/data/", 'key.json')
    client = get_client(json_key_file=json_key, readonly=True)

    # Submit an async query.
    job_id, _results = client.query('SELECT * FROM [bamboo-creek-195008:test2.Best_selling_10] LIMIT 1000')

    # Check if the query has finished running.
    complete, row_count = client.check_job(job_id)

    wait_until(lambda c=client, id=job_id: client.check_job(id), 10000)

    results = client.get_query_rows(job_id)
    return str(results)


@app.route("/chart")
@app.route("/")
def chart():
    s = qtest()
    data = ast.literal_eval(s)
    labels = []
    values = []
    for field in data:
        labels.append(field['Name_of_product'])
        values.append(field['Total_quantity'])
    return render_template('chart.html', values=values, labels=labels)


if __name__ == "__main__":
    app.run(host='127.0.0.1', port=8001)


