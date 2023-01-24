from flask import Flask, render_template, jsonify, abort, request, redirect
from flask_pymongo import PyMongo
import os
import re
from dotenv import load_dotenv
import yaml


app = Flask("RS_EVALUATION")
dotenv_path = os.path.join(app.instance_path, ".env")
load_dotenv(dotenv_path)

app.config["RS_EVALUATION_METRIC_DESC_DIR"] = os.environ.get(
    "RS_EVALUATION_METRIC_DESC_DIR"
)
app.config["MONGO_URI"] = os.environ.get("RS_EVALUATION_MONGO_URI")
mongo = PyMongo(app)


def load_sidebar_info():
    """Reads the available metric description yaml files in metric description
    folder path and creates dynamically a list of full names -> short names of
    metric descriptions
    in order to create automatically the appropriate links in sidebar
    """
    folder = app.config["RS_EVALUATION_METRIC_DESC_DIR"]
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
            app.config["RS_EVALUATION_METRIC_DESC_DIR"],
            e,
        )
    return {"metric_descriptions": desc}


app.sidebar_info = load_sidebar_info()


def respond_report_404(provider_name):
    return jsonify("Results for report: {} not found!"
                   .format(provider_name)), 404


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


def db_get_provider_names():
    """Get a list of the names of the providers handled in the system"""
    result = mongo.db.metrics.find({}, {"_id": 0, "provider": 1})
    providers = []
    for item in result:
        providers.append(item["provider"])
    return providers


def db_get_metrics(provider_name):
    """Get evaluated metric results from mongodb"""
    return mongo.db.metrics.find_one({"provider": provider_name}, {"_id": 0})


@app.route("/", strict_slashes=False)
def html_index():
    """Serve the main page that constructs the report view"""
    return render_template("./index.html")


@app.route("/ui", strict_slashes=False)
def html_default_report():
    """Select the first available provider and serve it's report as default"""
    default = db_get_provider_names()[0]
    return redirect("/ui/reports/{}".format(default), code=302)


@app.route("/ui/reports/<string:provider_name>", strict_slashes=False)
def html_metrics(provider_name):
    """Serve the main metrics dashboard"""
    reports = db_get_provider_names()
    if provider_name not in reports:
        abort(404)

    result = {}
    stats_needed = [
        "users",
        "recommended_items",
        "services",
        "user_actions",
        "user_actions_registered",
        "user_actions_registered_perc",
        "user_actions_anonymous",
        "user_actions_anonymous_perc",
        "user_actions_order",
        "user_actions_order_registered",
        "user_actions_order_registered_perc",
        "user_actions_order_anonymous",
        "user_actions_order_anonymous_perc",
        "start",
        "end",
    ]
    for stat_name in stats_needed:
        print(stat_name)
        result[stat_name] = get_statistic(provider_name, stat_name).get_json()

    metrics_needed = [
        "user_coverage",
        "catalog_coverage",
        "diversity",
        "diversity_gini",
        "novelty",
        "accuracy",
    ]

    for metric_name in metrics_needed:
        result[metric_name] = get_metric(provider_name, metric_name).get_json()

    result["timestamp"] = get_api_index(provider_name).get_json()["timestamp"]
    result["report"] = provider_name
    result["reports"] = reports
    result["sidebar_info"] = app.sidebar_info
    result["metric_active"] = None
    return render_template("./rsmetrics.html", data=result)


@app.route("/ui/reports/<string:provider_name>/kpis", strict_slashes=False)
def html_kpis(provider_name):
    """Serve html page about kpis per provider"""
    # call directly the get_metrics flask method implemented
    # in our api to get json about all metrics
    reports = db_get_provider_names()
    if provider_name not in reports:
        abort(404)

    result = {}

    stats_needed = ["start", "end"]
    for stat_name in stats_needed:
        result[stat_name] = get_statistic(provider_name, stat_name).get_json()

    metrics_needed = [
        "hit_rate",
        "click_through_rate",
        "top5_services_ordered",
        "top5_services_recommended",
    ]
    for metric_name in metrics_needed:
        result[metric_name] = get_metric(provider_name, metric_name).get_json()

    result["timestamp"] = get_api_index(provider_name).get_json()["timestamp"]
    result["sidebar_info"] = app.sidebar_info
    result["report"] = provider_name
    result["reports"] = reports
    result["metric_active"] = None

    return render_template("./kpis.html", data=result)


@app.route("/ui/reports/<string:provider_name>/graphs", strict_slashes=False)
def html_graphs(provider_name):
    """Serve html page about graphs per provider"""
    reports = db_get_provider_names()
    if provider_name not in reports:
        abort(404)

    result = {}

    stats_needed = ["start", "end"]
    for stat_name in stats_needed:
        result[stat_name] = get_statistic(provider_name, stat_name).get_json()

    result["timestamp"] = get_api_index(provider_name).get_json()["timestamp"]
    result["sidebar_info"] = app.sidebar_info
    result["report"] = provider_name
    result["reports"] = reports
    result["metric_active"] = None

    return render_template("./graphs.html", data=result)


@app.route("/ui/descriptions/metrics/<string:metric_name>",
           strict_slashes=False)
def html_metric_description(metric_name):
    """Serve html page about description of a specific metric"""
    reports = db_get_provider_names()
    result = {}

    # compose path to open correct yaml file
    dir = app.config["RS_EVALUATION_METRIC_DESC_DIR"]
    filename = metric_name + ".yml"
    try:
        with open(os.path.join(dir, filename), "r") as f:
            result = yaml.safe_load(f)
            result["sidebar_info"] = app.sidebar_info
            result["metric_active"] = metric_name
    except Exception as e:
        app.logger.error(
            "Could not load sidebar info from metric description folder:%s",
            app.config["RS_EVALUATION_METRIC_DESC_DIR"],
            e,
        )
        abort(404)
    # ref to know from which report metrics/kpis page were transitioned to here
    result["ref"] = request.args.get("ref")
    result["reports"] = reports
    return render_template("./metric_desc.html", data=result)


@app.route("/api/reports/<string:provider_name>")
def get_api_index(provider_name):
    """Serve metrics and statistics as default api response"""
    result = db_get_metrics(provider_name)
    return jsonify(result)


@app.route("/api/reports")
def get_reports():
    """Get provider names"""
    return jsonify(db_get_provider_names())


@app.route("/api/reports/<string:provider_name>/metrics")
def get_metrics(provider_name):
    """Serve the metrics data in json format"""
    result = db_get_metrics(provider_name)
    if not result:
        return respond_report_404(provider_name)
    return jsonify(result["metrics"])


@app.route("/api/reports/<string:provider_name>/metrics/<string:metric_name>")
def get_metric(provider_name, metric_name):
    """Serve specific metric data in json format"""
    result = db_get_metrics(provider_name)
    if not result:
        return respond_report_404(provider_name)
    for metric in result["metrics"]:
        if metric["name"] == metric_name:
            return jsonify(metric)
    return respond_metric_404(metric_name)


@app.route("/api/reports/<string:provider_name>/statistics")
def get_statistics(provider_name):
    """Serve the statistics data in json format"""
    result = db_get_metrics(provider_name)
    if not result:
        return respond_report_404(provider_name)
    return jsonify(result["statistics"])


@app.route("/api/reports/<string:provider_name>/statistics/<string:stat_name>")
def get_statistic(provider_name, stat_name):
    """Serve specific statistic data in json format"""
    result = db_get_metrics(provider_name)
    if not result:
        return respond_report_404(provider_name)
    for stat in result["statistics"]:
        if stat["name"] == stat_name:
            return jsonify(stat)
    return respond_stat_404(stat_name)


@app.route("/diag", strict_slashes=False)
def diag():
    return jsonify({"RS_metrics": {"status": "UP"}})
