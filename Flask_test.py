from flask import Markup
from flask import Flask
from flask import render_template, flash
from bigquery import get_client
from flask import (request, jsonify)
from flask_wtf import FlaskForm
from wtforms import SubmitField, StringField
from wtforms.validators import DataRequired

import os
import json
import time
import ast
import datetime

class StringForm(FlaskForm):
    department = StringField("Department: ")
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
    request_departments = """SELECT DateTimeS, Quantity FROM [bamboo-creek-195008:test2.SalesForLastYear] 
    WHERE Department = "%s" AND DATE(DateTimeS) >= "2017-01-01 00:00:00" AND DATE(DateTimeS) < "2018-01-01 00:00:00" AND Quantity > 0""" % (department)

    client = get_client(json_key_file = json_key, readonly = True)
    
    job_id, _results = client.query(request_departments)

    complete, row_count = client.check_job(job_id)

    wait_until(lambda c=client, id=job_id: client.check_job(id), 10000)

    results = client.get_query_rows(job_id)
    res = [0] * 53
    for el in results:
        res[datetime.datetime.strptime(el['DateTimeS'][:10], "%Y-%m-%d").isocalendar()[1]] += el['Quantity']

    return res

DEPARTMENTS = get_departments()

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

if __name__ == "__main__":
    app.run()
