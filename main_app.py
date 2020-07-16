import hashlib
import queue
import re
import time

import pymongo
import requests
from lxml import etree
from concurrent.futures.thread import ThreadPoolExecutor


class Main:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
        }
        self.city_list = self.get_citys()
        self.worker_pool = ThreadPoolExecutor(max_workers=50)
        self.qu_pool = ThreadPoolExecutor(max_workers=10)
        self.qu_queue = queue.Queue()
        self.run_qu_addr()
        self.myclient = pymongo.MongoClient("mongodb://139.196.91.125:27017/")

    def get_citys(self):
        url = 'https://www.ke.com/city/'
        resp = requests.get(url).text
        city_list = etree.HTML(resp).xpath('//li[@class="CLICKDATA"]/a/@href')
        return [i.replace('//', '') for i in city_list]

    def _exec_qu_detail(self, i):
        city_url = 'https://' + i + '/chengjiao'
        resp = requests.get(city_url, headers=self.headers).text
        qu_url_list = etree.HTML(resp).xpath('//div[@class="position"]//a[@class=" CLICKDATA"]/@href')
        qu_url_list = [city_url + i.replace('/chengjiao', '') for i in qu_url_list]
        for qu in qu_url_list:
            self.qu_queue.put(qu)

    def run_qu_addr(self):
        qu_url_list_ret = []
        for i in self.city_list:
            print(i)
            self.qu_pool.submit(self._exec_qu_detail, i)
        return qu_url_list_ret

    def run_qu_detail(self, url):
        mongo_db = self.myclient['beike']
        ret_list = []
        city = re.findall(r'https://(.*?).ke', url)[0]
        qu = re.findall(r'chengjiao/(.*?)/', url)[0]
        for i in range(100):
            t_url = url + '/pg{}/'.format(i + 1)
            resp = requests.get(t_url).text
            fang_url_list = etree.HTML(resp).xpath('//ul[@class="listContent"]/li')
            if fang_url_list == []:
                print(i, url, '没有了')
                return
            for li in fang_url_list:
                item = {}
                item['city'] = city
                item['qu'] = qu
                project_data = li.xpath('.//a[@class="CLICKDATA maidian-detail"]/text()')[0].split()
                item['project_name'] = project_data[0]
                item['huxin'] = project_data[1]
                item['mianji'] = project_data[2]
                item['dealDate'] = li.xpath('.//div[@class="dealDate"]/text()')[0].strip()
                item['dealPrice'] = li.xpath('.//div[@class="totalPrice"]/span/text()')[0].strip()
                item['danjia'] = li.xpath('.//div[@class="unitPrice"]/span/text()')[0].strip()
                item['guapaijia'] = li.xpath('.//span[@class="dealCycleTxt"]/span[1]/text()')[0].replace('挂牌',
                                                                                                         '').replace(
                    '万', '')
                item['zhouqi'] = li.xpath('.//span[@class="dealCycleTxt"]/span[2]/text()')[0].replace('成交周期',
                                                                                                      '').replace(
                    '天', '')
                item['jiangjia'] = (int(item['guapaijia']) - int(item['dealPrice'])) / int(item['guapaijia'])
                item['url'] = li.xpath('.//a[@class="CLICKDATA maidian-detail"]/@href')[0]
                item['_id'] = hashlib.md5(project_data + item['dealDate'] + item['dealPrice']).hexdigest()
                item['crawl_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item['caoxiang'] = li.xpath('.//div[@class="houseInfo"]/span/text()').strip('|')[0]
                item['zhuangxiu'] = li.xpath('.//div[@class="houseInfo"]/span/text()').strip('|')[1]
                print(i, item)
                mongo_db[item['city']].insert_one(item)
                ret_list.append(item)

    def _get_queue(self):
        while 1:
            if self.worker_pool._work_queue.qsize() > 50:
                time.sleep(1)
                continue
            url = self.qu_queue.get()
            print('队列还剩url：', self.qu_queue.qsize())
            print('拿到url', url)
            self.worker_pool.submit(self.run_qu_detail, url)


if __name__ == '__main__':
    app = Main()
    app._get_queue()
