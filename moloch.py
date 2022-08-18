#!/usr/bin/python
#-*- coding: utf-8 -*-




from elasticsearch import  Elasticsearch
import csv,time
from tqdm import tqdm


size = 2000
es = Elasticsearch(hosts='http://10.10.244.100:9200/',timeout=300)

hosts = [['10.10.240.0/24'],['10.62.0.0/16']],\
        [['10.10.240.0/24','10.10.243.127'],['10.80.0.0/16']],\
        [['10.10.242.10','10.10.242.11','10.10.242.80','10.10.242.81','10.10.240.0/24'],['10.22.0.0/16','10.23.0.0/16','10.26.16.0/24','10.30.0.0/16','10.31.0.0/16','10.35.0.0/16']
         ]


def handle2(info):
    srcIp = info['_source'].get('srcIp')
    dstIp = info['_source'].get('dstIp')
    srcPort = info['_source'].get('srcPort')
    dstPort = info['_source'].get('dstPort')
    protocol = info['_source'].get('protocol')
    csv_writer.writerow([srcIp, dstIp, srcPort, dstPort, protocol])


def getdata(srcip,destip):
    print(srcip,destip)
    datas = []

    bodys = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"srcIp": srcip}},
                    {"match": {"dstIp": destip}}
                ],
                # "filter": {
                #     "range": {
                #         "timestamp": {
                #             "gte": "1644681600000",
                            # "lte": "1644768000000"
                #         }
                #     }
                # }
            }
        }
    }


    # bodys = {'query':{"match_all":{}}}

    result = es.search(index='sessions2-220317', body=bodys, scroll="60m", size=size)
    scrollid = result['_scroll_id']

    total = result['hits']['total']['value']
    print(total)
    pagenum = total // size + 1

    datas += result['hits']['hits']


    if pagenum > 1:
        for _ in tqdm(range(pagenum)):
            try:
                result = es.scroll(scroll_id=scrollid, scroll="60m")
            except Exception as e:
                print(e)
            datas += result['hits']['hits']
    es.clear_scroll(scroll_id=scrollid)

    return  datas


for zone in hosts:
    for src in zone[0]:
        for dest in zone[1]:
            # datas = getdata(src,dest)  #本地到云上
            datas = getdata(dest,src)    #云上到本地

            if len(datas) == 0:
                print('没有数据')
                continue

            # filename = 'csv/%s-%s.csv' % (src.replace('/', '_'), dest.replace('/', '_'))    #本地到云上
            filename = 'csv/%s-%s.csv' % (dest.replace('/', '_'), src.replace('/', '_'))  #云上到本地
            f1 = open(filename, 'a', encoding='utf-8', newline='')
            csv_writer = csv.writer(f1)
            csv_writer.writerow(['srcIp', 'dstIp', 'srcPort', 'dstPort', 'protocol'])
            for i in tqdm(datas):
                handle2(i)
            f1.close()