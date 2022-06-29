from flask import Flask, render_template, jsonify
import json, os
from dotenv import load_dotenv


app = Flask('RS_EVALUATION')
dotenv_path = os.path.join(app.instance_path, '.env')
load_dotenv(dotenv_path)

app.config['RS_EVALUATION_METRICS'] = os.environ.get('RS_EVALUATION_METRICS')


@app.route("/")
def main_page():
    '''Serve the main page that constructs the report view'''
    # Render the report template and specifiy metric resource to be '/api' since the report is hosted in the webservice
    return render_template('./report.html.prototype',metric_source='/api')   


@app.route("/api")
def get_api_index():
    '''Serve metrics and statistics as default api response'''
    result = {}

    with open(app.config['RS_EVALUATION_METRICS'], 'r') as f:
      result = json.load(f)
      f.close()
    return jsonify(result)


@app.route("/api/metrics")
def get_metrics():
    '''Serve the metrics data in json format'''
    result = {}

    with open(app.config['RS_EVALUATION_METRICS'], 'r') as f:
      result = json.load(f)
      f.close()
    return jsonify(result['metrics'])

@app.route("/api/metrics/<string:metric_name>")
def get_metric(metric_name):
    '''Serve specific metric data in json format'''
    result = {}

    with open(app.config['RS_EVALUATION_METRICS'], 'r') as f:
      result = json.load(f)
      f.close()
    
    for metric in result['metrics']:
      if metric['name'] == metric_name:
        return jsonify(metric)
    
    return jsonify({'code':404,'error':'metric with name: {} does not exist!'.format(metric_name)}),404

@app.route("/api/statistics")
def get_statistics():
    '''Serve the statistics data in json format'''
    result = {}

    with open(app.config['RS_EVALUATION_METRICS'], 'r') as f:
      result = json.load(f)
      f.close()
    return jsonify(result['statistics'])

@app.route("/api/statistics/<string:stat_name>")
def get_statistic(stat_name):
    '''Serve specific statistic data in json format'''
    result = {}

    with open(app.config['RS_EVALUATION_METRICS'], 'r') as f:
      result = json.load(f)
      f.close()
    
    for stat in result['statistics']:
      if stat['name'] == stat_name:
        return jsonify(stat)
    
    return jsonify({'code':404,'error':'metric with name: {} does not exist!'.format(stat_name)}),404