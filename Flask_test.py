from flask import Markup
from flask import Flask
from flask import render_template, flash
from bigquery import get_client
from flask import (request, jsonify)
from flask_wtf import FlaskForm
from wtforms import SelectField, SubmitField, StringField
from wtforms.validators import DataRequired

import os
import json
import time
import ast
import datetime

choices_x = [("ProductID", "ProductID"), ("Quantity", "Quantity"), ("Price", "Price"), ("ProductName", "ProductName"), ("DCSS", "DCSS")]
choices_y = [("ProductID", "ProductID"), ("Quantity", "Quantity"), ("Price", "Price")]
n_choices_x = ("ProductID", "Quantity", "Price", "ProductName", "DCSS")
n_choices_y = ("ProductID)", "Quantity", "Price")

class StringForm(FlaskForm):
    department = StringField("Department: ")
    submit = SubmitField("Get Data")

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


def get_departments():
    request_departments = """SELECT Department FROM [bamboo-creek-195008:test2.SalesForLastYear] 
    WHERE DATE(DateTimeS) >= "2017-01-01 00:00:00" AND DATE(DateTimeS) < "2018-01-01 00:00:00" 
    GROUP BY Department"""

    client = get_client(json_key_file = json_key, readonly = True)
    
    job_id, _results = client.query(request_departments)

    complete, row_count = client.check_job(job_id)

    wait_until(lambda c=client, id=job_id: client.check_job(id), 10000)

    results = client.get_query_rows(job_id)

    res = [el['Department'] for el in results]

    return res

def get_dates(department):
    request_departments = """SELECT DateTimeS FROM [bamboo-creek-195008:test2.SalesForLastYear] 
    WHERE Department = "%s" AND DATE(DateTimeS) >= "2017-01-01 00:00:00" AND DATE(DateTimeS) < "2018-01-01 00:00:00" """ % (department)

    client = get_client(json_key_file = json_key, readonly = True)
    
    job_id, _results = client.query(request_departments)

    complete, row_count = client.check_job(job_id)

    wait_until(lambda c=client, id=job_id: client.check_job(id), 10000)

    results = client.get_query_rows(job_id)
    res = [0] * 53
    for el in results:
        res[datetime.datetime.strptime(el['DateTimeS'][:10], "%Y-%m-%d").isocalendar()[1]] += 1

    return res

DEPARTMENTS = get_departments()

def qtest(first, second):
    client = get_client(json_key_file=json_key, readonly=True)

    # Submit an async query.
    job_id, _results = client.query('SELECT %s, %s FROM [bamboo-creek-195008:test2.OleSmokey3] LIMIT 10' % (first, second))

    # Check if the query has finished running.
    complete, row_count = client.check_job(job_id)

    wait_until(lambda c=client, id=job_id: client.check_job(id), 10000)

    results = client.get_query_rows(job_id)
    return str(results)



@app.route('/query2', methods = ['GET', 'POST'])
def queryPage2():
    form = StringForm()

    if request.method == 'POST':
        if form.department.data == "":
            flash("Empty field is not required")
            return render_template('query_page2.html', form=form)

        if form.department.data not in DEPARTMENTS:
            flash("No such fields")
            return render_template('query_page2.html', form=form)

        data = get_dates(form.department.data)
        labels = list(range(1, 53))
        return render_template('chart.html', values=data, labels=labels)

    return render_template('query_page2.html', form=form)

@app.route('/query', methods = ['GET', 'POST'])
def queryPage():
    form = SelectForm()

    if request.method == 'POST':
        if form.x_axes.data == form.y_axes.data:
            flash("Same fields are not required")
            return render_template('query_page.html', form = form)

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
