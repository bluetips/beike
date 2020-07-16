import re
from concurrent.futures.thread import ThreadPoolExecutor

import pymongo
import requests
from lxml import etree


class Tool:
    def __init__(self):
        self.myclient = pymongo.MongoClient("mongodb://139.196.91.125:27017/")
        self.db = self.myclient['beike']
        self.db_1 = self.myclient['beike_city']
        self.city_data = {}
        self.qu_pool = ThreadPoolExecutor(max_workers=10)
        self.worker_pool = ThreadPoolExecutor(max_workers=50)

    def get_citys(self):
        url = 'https://www.ke.com/city/'
        resp = requests.get(url).text
        city_list = etree.HTML(resp).xpath('//li[@class="CLICKDATA"]/a/@href')
        for i in etree.HTML(resp).xpath('//li[@class="CLICKDATA"]/a'):
            self.city_data[i.xpath('./@href')[0].replace('.ke.com', '').replace('//', '')] = i.xpath('./text()')[0]
        return [i.replace('//', '') for i in city_list]

    def _exec_qu_detail(self, i):
        city_url = 'https://' + i + '/chengjiao'
        city = re.findall(r'https://(.*?).ke', city_url)[0]
        city_name = self.city_data[city]
        resp = requests.get(city_url).text
        qu_url_list = etree.HTML(resp).xpath('//div[@class="position"]//a[@class=" CLICKDATA"]')
        for qu in qu_url_list:
            item = {}
            try:
                item['code'] = re.findall(r'/chengjiao/(.*?)/', qu.xpath('./@href')[0])[0]
            except Exception:
                break
            item['city'] = city_name
            item['qu'] = qu.xpath('./text()')[0]
            print(item)
            self.db_1['city_info'].insert_one(item)

    def run_qu_addr(self):
        qu_url_list_ret = []
        for i in self.city_list:
            print(i)
            self.qu_pool.submit(self._exec_qu_detail, i)
            # self._exec_qu_detail(i)
        return qu_url_list_ret

    def get_city_data(self):
        self.city_list = self.get_citys()
        self.run_qu_addr()

    def get_qu_data(self, code):
        return self.db_1['city_info'].find_one({'code': code})

    def _exec_work(self, co):
        db = self.myclient['beike'][co]
        ret = db.find()
        headers = ['class', 'name', 'sex', 'height', 'year']
        f = open('{}.csv'.format(co),'w')
        for i in ret:
            pass

    def out(self):
        collections = self.db.collection_names()
        for co in collections:
            self._exec_work(co)


if __name__ == '__main__':
    app = Tool()
    # app.get_city_data()
    app.out()
