from flask import Flask, render_template, jsonify, abort, request, redirect
from flask_pymongo import PyMongo, pymongo
import xmlrpc.client
import os
import re
from dotenv import load_dotenv
import yaml


app = Flask("RSEVAL")

dotenv_path = os.path.join(app.instance_path, ".env")
load_dotenv(dotenv_path)

app.config['JSON_SORT_KEYS'] = False
app.config["RSEVAL_METRIC_DESC_DIR"] = os.environ.get(
    "RSEVAL_METRIC_DESC_DIR"
)
app.config["RSEVAL_STREAM_USER_ACTIONS_JOBNAME"] = os.environ.get(
    "RSEVAL_STREAM_USER_ACTIONS_JOBNAME"
)
app.config["RSEVAL_STREAM_RECOMMENDATIONS_JOBNAME"] = os.environ.get(
    "RSEVAL_STREAM_RECOMMENDATIONS_JOBNAME"
)
app.config["RSEVAL_STREAM_MP_DB_EVENTS_JOBNAME"] = os.environ.get(
    "RSEVAL_STREAM_MP_DB_EVENTS_JOBNAME"
)

app.config["MONGO_URI"] = os.environ.get("RSEVAL_MONGO_URI")
mongo = PyMongo(app)


def load_sidebar_info():
    """Reads the available metric description yaml files in metric description
    folder path and creates dynamically a list of full names -> short names of
    metric descriptions
    in order to create automatically the appropriate links in sidebar
    """
    folder = app.config["RSEVAL_METRIC_DESC_DIR"]
    desc = {}
    app.logger.info(
        "Opening metric description folder %s to gather sidebar info...",
        folder
    )
    try:
        for filename in os.listdir(folder):
            if filename.endswith(".yml"):
                with open(os.path.join(folder, filename), "r") as f:
                    app.logger.info("Opening metric description file %s",
                                    filename)
                    result = yaml.safe_load(f)
                    # Remove .yml suffix from filename
                    name = re.sub(r"\.yml$", "", filename)
                    desc[name] = {"fullname": result["name"],
                                  "style": result["style"]}
    except Exception as e:
        app.logger.error(
            "Could not load sidebar info from metric description folder:%s",
            app.config["RSEVAL_METRIC_DESC_DIR"],
            e,
        )
    return {"metric_descriptions": desc}


app.sidebar_info = load_sidebar_info()


def respond_report_404(report_name):
    return jsonify("Results for report: {} not found!"
                   .format(report_name)), 404


def respond_metric_404(metric_name):
    return (
        jsonify(
            {
                "code": 404,
                "error": "metric with name: {} does not exist!"
                .format(metric_name),
            }
        ),
        404,
    )


def respond_stat_404(stat_name):
    return (
        jsonify(
            {
                "code": 404,
                "error": "statistic with name: {} does not exist!"
                .format(stat_name),
            }
        ),
        404,
    )


def db_get_report_names():
    """Get a list of the names of the reports handled in the system
    sorted by the most recent evaluated report
    """
    result = mongo.db.metrics.find(
        {}, {"name": 1}).sort("_id", -1)
    reports = []
    for item in result:
        reports.append(item["name"])
    return reports


def db_get_metrics(report_name):
    """Get evaluated metric results from mongodb"""
    return mongo.db.metrics.find_one({"name": report_name}, {"_id": 0})


@app.route("/", strict_slashes=False)
def html_index():
    """Serve the main page that constructs the report view"""
    return render_template("./index.html")


@app.route("/ui", strict_slashes=False)
def html_default_report():
    """Select the first available provider and serve it's report as default"""
    default = db_get_report_names()[0]
    return redirect("/ui/reports/{}".format(default), code=302)


@app.route("/ui/reports/<string:report_name>", strict_slashes=False)
def html_metrics(report_name):
    """Serve the main metrics dashboard"""
    reports = db_get_report_names()
    if report_name not in reports:
        abort(404)

    result = {}
    stats_needed = [
        "registered_users",
        "anonymous_users",
        "recommended_items",
        "items",
        "user_actions",
        "user_actions_all",
        "user_actions_registered",
        "user_actions_registered_perc",
        "user_actions_anonymous",
        "user_actions_anonymous_perc",
        "item_views_all",
        "item_views",
        "item_views_registered",
        "item_views_registered_perc",
        "item_views_anonymous",
        "item_views_anonymous_perc",
        "start",
        "end",
    ]
    for stat_name in stats_needed:
        print(stat_name)
        result[stat_name] = get_statistic(report_name, stat_name).get_json()

    metrics_needed = [
        "user_coverage",
        "catalog_coverage",
        "diversity",
        "diversity_gini",
        "novelty",
        "accuracy",
    ]

    for metric_name in metrics_needed:
        result[metric_name] = get_metric(report_name, metric_name).get_json()

    data = get_api_index(report_name).get_json()
    result["timestamp"] = data.get("timestamp")
    result["errors"] = data.get("errors")
    result["report"] = report_name
    result["reports"] = reports
    result["sidebar_info"] = app.sidebar_info
    result["metric_active"] = None
    return render_template("./rsmetrics.html", data=result)


@app.route("/ui/reports/<string:report_name>/kpis", strict_slashes=False)
def html_kpis(report_name):
    """Serve html page about kpis per provider"""
    # call directly the get_metrics flask method implemented
    # in our api to get json about all metrics
    reports = db_get_report_names()
    if report_name not in reports:
        abort(404)

    result = {}

    stats_needed = ["start", "end"]
    for stat_name in stats_needed:
        result[stat_name] = get_statistic(report_name, stat_name).get_json()

    metrics_needed = [
        "hit_rate",
        "click_through_rate",
        "top5_items_viewed",
        "top5_items_recommended",
        "top5_categories_viewed",
        "top5_categories_recommended",
        "top5_scientific_domains_viewed",
        "top5_scientific_domains_recommended",
    ]
    for metric_name in metrics_needed:
        result[metric_name] = get_metric(report_name, metric_name).get_json()

    data = get_api_index(report_name).get_json()
    result["timestamp"] = data.get("timestamp")
    result["errors"] = data.get("errors")
    result["sidebar_info"] = app.sidebar_info
    result["report"] = report_name
    result["reports"] = reports
    result["metric_active"] = None

    return render_template("./kpis.html", data=result)


@app.route("/ui/reports/<string:report_name>/graphs",
           strict_slashes=False)
def html_graphs(report_name):
    """Serve html page about graphs per provider"""
    reports = db_get_report_names()
    if report_name not in reports:
        abort(404)

    result = {}

    stats_needed = ["start", "end"]
    for stat_name in stats_needed:
        result[stat_name] = get_statistic(report_name, stat_name).get_json()

    data = get_api_index(report_name).get_json()
    result["timestamp"] = data.get("timestamp")
    result["errors"] = data.get("errors")
    result["sidebar_info"] = app.sidebar_info
    result["report"] = report_name
    result["reports"] = reports
    result["metric_active"] = None

    return render_template("./graphs.html", data=result)


@app.route("/ui/descriptions/metrics/<string:metric_name>",
           strict_slashes=False)
def html_metric_description(metric_name):
    """Serve html page about description of a specific metric"""
    reports = db_get_report_names()
    result = {}

    # compose path to open correct yaml file
    dir = app.config["RSEVAL_METRIC_DESC_DIR"]
    filename = metric_name + ".yml"
    try:
        with open(os.path.join(dir, filename), "r") as f:
            result = yaml.safe_load(f)
            result["sidebar_info"] = app.sidebar_info
            result["metric_active"] = metric_name
    except Exception as e:
        app.logger.error(
            "Could not load sidebar info from metric description folder:%s",
            app.config["RSEVALMETRIC_DESC_DIR"],
            e,
        )
        abort(404)
    # ref to know from which report metrics/kpis page were transitioned to here
    result["ref"] = request.args.get("ref")
    result["reports"] = reports
    return render_template("./metric_desc.html", data=result)


@app.route("/api/reports/<string:report_name>")
def get_api_index(report_name):
    """Serve metrics and statistics as default api response"""
    result = db_get_metrics(report_name)
    return jsonify(result)


@app.route("/api/reports")
def get_reports():
    """Get provider names"""
    return jsonify(db_get_report_names())


@app.route("/api/reports/<string:report_name>/metrics")
def get_metrics(report_name):
    """Serve the metrics data in json format"""
    result = db_get_metrics(report_name)
    if not result:
        return respond_report_404(report_name)
    return jsonify(result["metrics"])


@app.route("/api/reports/<string:report_name>/metrics/<string:metric_name>")
def get_metric(report_name, metric_name):
    """Serve specific metric data in json format"""
    result = db_get_metrics(report_name)
    if not result:
        return respond_report_404(report_name)
    for metric in result["metrics"]:
        if metric["name"] == metric_name:
            return jsonify(metric)
    return respond_metric_404(metric_name)


@app.route("/api/reports/<string:report_name>/statistics")
def get_statistics(report_name):
    """Serve the statistics data in json format"""
    result = db_get_metrics(report_name)
    if not result:
        return respond_report_404(report_name)
    return jsonify(result["statistics"])


@app.route("/api/reports/<string:report_name>/statistics/<string:stat_name>")
def get_statistic(report_name, stat_name):
    """Serve specific statistic data in json format"""
    result = db_get_metrics(report_name)
    if not result:
        return respond_report_404(report_name)
    for stat in result["statistics"]:
        if stat["name"] == stat_name:
            return jsonify(stat)
    return respond_stat_404(stat_name)


@app.route("/diag", strict_slashes=False)
def diag():
    """Health check"""

    def print_status(value):
        if value == 1:
            return "UP"
        elif value == 0:
            return "DOWN"
        return "UNKNOWN"

    # begin with statused deemed as DOWN and then check if they are UP and
    # update. Api statusis deemed up since executing this call means API is
    # indeed working

    general_status = 0
    api_status = 1
    mongo_status = 0
    stream_status = 0
    stream_ua = 0
    stream_mpdb = 0
    stream_rec = 0

    # check mongo connectivity with a sensible timeout of 3sec
    mongo_check = PyMongo(
        app, uri=app.config["MONGO_URI"], serverSelectionTimeoutMS=3000
    )
    try:
        mongo_check.cx.admin.command("ping")
        mongo_status = 1
    except pymongo.errors.ServerSelectionTimeoutError:
        app.logger.error("Error trying to check mongodb connectivity")

    # check supervisor connectivity
    try:
        rpc_srv = xmlrpc.client.ServerProxy('http://localhost:9001/RPC2')

        # if connected get supervisor status
        supervisor = rpc_srv.supervisor.getState()
        if supervisor["statecode"]:
            job_ua_info = rpc_srv.supervisor.getProcessInfo(
                app.config["RSEVAL_STREAM_USER_ACTIONS_JOBNAME"]
            )
            if job_ua_info["statename"] == "RUNNING":
                stream_ua = 1

            job_rec_info = rpc_srv.supervisor.getProcessInfo(
                app.config["RSEVAL_STREAM_RECOMMENDATIONS_JOBNAME"]
            )
            if job_rec_info["statename"] == "RUNNING":
                stream_rec = 1

            job_mpdb_info = rpc_srv.supervisor.getProcessInfo(
                app.config["RSEVAL_STREAM_MP_DB_EVENTS_JOBNAME"]
            )
            if job_mpdb_info["statename"] == "RUNNING":
                stream_mpdb = 1

    except (ConnectionRefusedError, xmlrpc.client.Fault):
        app.logger.error(
            "Error trying to check supervisord connectivity and job statuses"
        )

    # aggregate results
    stream_status = stream_ua * stream_mpdb
    # streaming being unavailable doesn't affect the RSEVAL ui/api to display
    # results. we should use another metric to affect general status if
    # streaming is absent for quite a while and the data is stale
    general_status = api_status * mongo_status

    result = {
        "RS_metrics": {
            "status": print_status(general_status),
            "RS_metrics_api": {"status": print_status(api_status)},
            "RS_metrics_datastore": {"status": print_status(mongo_status)},
            "RS_streaming": {
                "status": print_status(stream_status),
                "RS_streaming_user_actions": {
                    "status": print_status(stream_ua)
                },
                "RS_streaming_recommendations": {
                    "status": print_status(stream_rec)
                },
                "RS_streaming_mp_events": {
                    "status": print_status(stream_mpdb)
                }
            },
        }
    }

    return jsonify(result)
