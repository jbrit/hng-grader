from flask import Blueprint, request, render_template, send_from_directory
import requests
import time
import concurrent.futures
import pandas as pd
import os

api_blueprint = Blueprint("api", __name__)

def get_data(url):
    # expecting a url to be passed in with fields
    # slackUsername: str
    # age: int
    # backend: True
    # bio: str

    return_response = {
        "url": url,
        "slackUsername": None,
        "age": None,
        "backend": None,
        "bio": None,
        "error": None
    }

    try:
        response = requests.get(url, timeout=10).json()

        if "slackUsername" not in response or type(response["slackUsername"]) != str:
            return_response["error"] = "slackUsername is missing or not a string"
            raise Exception(return_response["error"])

        if "age" not in response or type(response["age"]) != int:
            return_response["error"] = "age is missing or not an integer"
            raise Exception(return_response["error"])

        if "backend" not in response or response["backend"] != True:
            return_response["error"] = "backend is missing or not true"
            raise Exception(return_response["error"])

        if "bio" not in response or type(response["bio"]) != str:
            return_response["error"] = "bio is missing or not a string"
            raise Exception(return_response["error"])

        return_response["slackUsername"] = response["slackUsername"]
        return_response["age"] = response["age"]
        return_response["backend"] = response["backend"]
        return_response["bio"] = response["bio"]

        return return_response
    except Exception as e:
        return_response["error"] = str(e).strip() if str(e).strip() != "" else "Error getting data from url"
        return return_response


def get_data_from_urls(urls):
    with concurrent.futures.ThreadPoolExecutor() as executor:

        futures = []

        for url in urls:
            futures.append(executor.submit(get_data, url=url))
        
        results = []
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())

        return results

@api_blueprint.route('/', methods=('GET', 'POST'))
def register():
    if request.method == 'POST':
        # get file from request
        try:
            file = request.files['csv']
            pd_file = pd.read_csv(file)
            # get api url key, it's at the 5th indexed colum
            url_key = pd_file.columns[5]

            # first row is nan
            urls = pd_file[url_key].tolist()[:15]

        except Exception as e:
            print(e)
            return render_template("home.html", error="Error getting file from request")

        tm1 = time.perf_counter()
        print(len(urls))
        results = get_data_from_urls(urls)
        # filter out results with errors
        results_with_errors = [result for result in results if result["error"] is not None]
        results_without_errors = [result for result in results if result["error"] is None]
        tm2 = time.perf_counter()
        print(f"Finished in {tm2-tm1} seconds")
        # create dataframe from results
        df = pd.DataFrame(results_without_errors)
        # create dataframe from results with errors
        df_errors = pd.DataFrame(results_with_errors)
        # create csv from dataframe
        df.to_csv("normal.csv", index=False)
        # create csv from dataframe with errors
        df_errors.to_csv("errors.csv", index=False)
        context = {
            "loaded": True
        }
        return render_template("home.html", **context)
    return render_template('home.html')

@api_blueprint.route("/<path:name>")
def download_file(name):
    files = {
        "normal": "normal.csv",
        "errors": "errors.csv",
    }
    if name not in files:
        return "File not found", 404
    # get current directory
    directory = os.path.dirname(os.path.realpath(__file__))
    # get file path
    print(directory)
    return send_from_directory(directory=directory, path=files[name], as_attachment=True)