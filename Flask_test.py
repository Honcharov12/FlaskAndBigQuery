from flask import Markup
from flask import Flask
from flask import render_template
from bigquery import get_client
from flask import (request, jsonify)
from flask_wtf import FlaskForm
from wtforms import SelectField, SubmitField
from wtforms.validators import DataRequired

import os
import json
import time
import ast

choices_x = [("ProductID", "ProductID"), ("Quantity", "Quantity"), ("Price", "Price"), ("ProductName", "ProductName"), ("DCSS", "DCSS")]
choices_y = [("ProductID", "ProductID"), ("Quantity", "Quantity"), ("Price", "Price")]

class SelectForm(FlaskForm):
    x_axes = SelectField("Choice 1: ", choices = choices_x)
    y_axes = SelectField("Choice 2: ", choices = choices_y)
    submit = SubmitField("Get Data")

def wait_until(somepredicate, timeout, period=0.25, *args, **kwargs):
    mustend = time.time() + timeout
    while time.time() < mustend:
        if somepredicate(*args, **kwargs):
            return True
        time.sleep(period)
    return False


app = Flask(__name__)
app.secret_key = 'key'
json_key = os.path.join(os.getcwd() + "/data/", 'key.json')


def qtest(first, second):
    client = get_client(json_key_file=json_key, readonly=True)

    # Submit an async query.
    job_id, _results = client.query('SELECT %s, %s FROM [bamboo-creek-195008:test2.OleSmokey3] LIMIT 10' % (first, second))

    # Check if the query has finished running.
    complete, row_count = client.check_job(job_id)

    wait_until(lambda c=client, id=job_id: client.check_job(id), 10000)

    results = client.get_query_rows(job_id)
    return str(results)




@app.route('/query', methods = ['GET', 'POST'])
def queryPage():
    form = SelectForm()

    if request.method == 'POST':
        values, labels = executeQuery(form.x_axes.data, form.y_axes.data)
        return render_template('chart.html', values=values, labels=labels)

    return render_template('query_page.html', form=form)


# not implemented
def executeQuery(x, y):
    s = qtest(x, y)
    data = ast.literal_eval(s)
    labels = []
    values = []
    for field in data:
        labels.append(field[x])
        values.append(field[y])
    return labels, values


@app.route('/ajax', methods=['POST'])
def ajax_request():
    query = request.form['query']
    res = executeQuery(query)
    return jsonify(returnedData=res)


if __name__ == "__main__":
    app.run(host='127.0.0.1', port=8001)
