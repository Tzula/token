#!/usr/bin/env python
# -*- coding: utf-8 -*-
from service import get_info
from flask import Flask, render_template
import json
app = Flask(__name__)


@app.route('/')
def hello_world():
    ret =get_info.get_allwebinfo()

    return render_template('indexnew.html', ret=ret)

@app.route("/<string:key>/", methods=["GET", "POST"])
def detail(key):
    # print(key)
    if key == 'favicon.ico':
        return ""
    elif key == 'recommended-affiliate-networks':
        return render_template('affiliate_networks.html')
    elif key == 'recommended-tracking-software':
        return render_template('tracking_software.html')
    elif key == 'what-is-a-token':
        return render_template('is_token.html')
    ret = get_info.get_detailinfo(key)
    # print(ret)
    minititle = ret[0][0]
    title = ret[0][1]
    macros_num = ret[0][2]
    traffic_platform = ret[0][3]
    token_name = ret[0][4]
    token_name = json.loads(token_name)
    token_description = ret[0][5]
    token_description = json.loads(token_description)
    conversion_tracking = ret[0][6]
    panel_body = ret[0][7]
    webname = ret[0][8]
    num = len(token_name)
    return render_template('content.html', minititle=minititle, title=title, macros_num=macros_num,
                           traffic_platform=traffic_platform, token_name=token_name, token_description=token_description,
                           conversion_tracking=conversion_tracking, panel_body=panel_body, num=num, webname=webname)


if __name__ == '__main__':
    app.run(port=8081, host='0.0.0.0')
