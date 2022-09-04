from flask import Flask, render_template, jsonify, abort
import json, os, re 
from dotenv import load_dotenv
import yaml



app = Flask('RS_EVALUATION')
dotenv_path = os.path.join(app.instance_path, '.env')
load_dotenv(dotenv_path)



app.config['RS_EVALUATION_METRICS_FILE'] = os.environ.get('RS_EVALUATION_METRICS_FILE')
app.config['RS_EVALUATION_METRIC_DESC_DIR'] = os.environ.get('RS_EVALUATION_METRIC_DESC_DIR')

def load_sidebar_info():
  '''Reads the available metric description yaml files in metric description folder path
  and creates dynamically a list of full names -> short names of metric descriptions
  in order to create automatically the appropriate links in sidebar
  '''
  folder = app.config['RS_EVALUATION_METRIC_DESC_DIR']
  desc = {}
  app.logger.info('Opening metric description folder %s to gather sidebar info...',folder)
  try:
    for filename in os.listdir(folder):
      if filename.endswith(".yml"):
        with open(os.path.join(folder,filename), 'r') as f:
          app.logger.info('Opening metric description file %s',filename)
          result = yaml.safe_load(f)
          # Remove .yml suffix from filename
          name=re.sub('\.yml$', '', filename)
          desc[name]= { 'fullname': result['name'], 'style': result['style']}
          f.close()
  except:
    app.logger.error('Could not load sidebar info from metric description folder:%s',app.config['RS_EVALUATION_METRIC_DESC'])
  return {'metric_descriptions':desc}

app.sidebar_info = load_sidebar_info()


@app.route("/")
def html_main():
    '''Serve the main page that constructs the report view'''
    # Render the main dashboard
    result = {}
    stats_needed = ['users', 'recommendations', 'services', 'user_actions', 
    'user_actions_registered', 'user_actions_registered_perc', 
    'user_actions_anonymous', 'user_actions_anonymous_perc',
    'user_actions_order', 'user_actions_order_registered', 'user_actions_order_registered_perc',
    'user_actions_order_anonymous','user_actions_order_anonymous_perc','start','end']
    for stat_name in stats_needed:
      result[stat_name] = get_statistic(stat_name).get_json()
    
    metrics_needed = ['user_coverage', 'catalog_coverage', 'diversity', 'diversity_gini', 'novelty']
    
    for metric_name in metrics_needed:  
      result[metric_name] = get_metric(metric_name).get_json()

    result['timestamp'] = get_api_index().get_json()['timestamp']

    result['sidebar_info'] = app.sidebar_info
    result['metric_active'] = None
    return render_template('./rsmetrics.html',data=result)   

@app.route("/kpis")
def html_kpis():
    '''Serve html page about kpis'''
    # call directly the get_metrics flask method implemented in our api to get json about all metrics
    result = {}

    stats_needed = ['start','end']
    for stat_name in stats_needed:
      result[stat_name] = get_statistic(stat_name).get_json()

    metrics_needed = ['hit_rate', 'click_through_rate', 'top5_services_ordered', 'top5_services_recommended']   
    for metric_name in metrics_needed:
      result[metric_name] = get_metric(metric_name).get_json()

    result['timestamp'] = get_api_index().get_json()['timestamp']
    result['sidebar_info'] = app.sidebar_info
    result['metric_active'] = None

    return render_template('./kpis.html', data=result)


@app.route("/descriptions/metrics/<string:metric_name>")
def html_metric_description(metric_name):
    '''Serve html page about description of a specific metric'''
    result = {}

    # compose path to open correct yaml file 
    dir = app.config['RS_EVALUATION_METRIC_DESC_DIR']
    filename = metric_name + ".yml"
    try:
      with open(os.path.join(dir,filename), 'r') as f:
        result = yaml.safe_load(f)
        result['sidebar_info'] = app.sidebar_info
        result['metric_active'] = metric_name
        f.close()
    except:
      abort(404)

    return render_template('./metric_desc.html', data=result)



@app.route("/api")
def get_api_index():
    '''Serve metrics and statistics as default api response'''
    result = {}

    with open(app.config['RS_EVALUATION_METRICS_FILE'], 'r') as f:
      result = json.load(f)
      f.close()
    return jsonify(result)


@app.route("/api/metrics")
def get_metrics():
    '''Serve the metrics data in json format'''
    result = {}

    with open(app.config['RS_EVALUATION_METRICS_FILE'], 'r') as f:
      result = json.load(f)
      f.close()
    return jsonify(result['metrics'])

@app.route("/api/metrics/<string:metric_name>")
def get_metric(metric_name):
    '''Serve specific metric data in json format'''
    result = {}

    with open(app.config['RS_EVALUATION_METRICS_FILE'], 'r') as f:
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

    with open(app.config['RS_EVALUATION_METRICS_FILE'], 'r') as f:
      result = json.load(f)
      f.close()
    return jsonify(result['statistics'])

@app.route("/api/statistics/<string:stat_name>")
def get_statistic(stat_name):
    '''Serve specific statistic data in json format'''
    result = {}

    with open(app.config['RS_EVALUATION_METRICS_FILE'], 'r') as f:
      result = json.load(f)
      f.close()
    
    for stat in result['statistics']:
      if stat['name'] == stat_name:
        return jsonify(stat)
    
    return jsonify({'code':404,'error':'metric with name: {} does not exist!'.format(stat_name)}),404