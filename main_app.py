import queue
import re
import time

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

    def get_citys(self):
        url = 'https://www.ke.com/city/'
        resp = requests.get(url).text
        city_list = etree.HTML(resp).xpath('//li[@class="CLICKDATA"]/a/@href')
        return [i.replace('//', '') for i in city_list]

    def _exec_qu_detail(self, i):
        city_url = 'https://' + i + '/ershoufang'
        resp = requests.get(city_url, headers=self.headers).text
        qu_url_list = etree.HTML(resp).xpath('//div[@class="position"]//a[@class=" CLICKDATA"]/@href')
        qu_url_list = [city_url + i.replace('/ershoufang', '') for i in qu_url_list]
        for qu in qu_url_list:
            self.qu_queue.put(qu)

    def run_qu_addr(self):
        qu_url_list_ret = []
        for i in self.city_list:
            print(i)
            self.qu_pool.submit(self._exec_qu_detail, i)
        return qu_url_list_ret

    def run_qu_detail(self, url):
        for i in range(100):
            t_url = url + '/pg{}/'.format(i + 1)
            resp = requests.get(t_url, headers=self.headers).text
            fang_url = etree.HTML(resp).xpath('//div')
            pass

    def run(self):
        time.sleep(1)
        while self.qu_queue.qsize() != 0 or self.qu_pool._work_queue.qsize() != 0:
            url = self.qu_queue.get()
            self.worker_pool.submit(self.run_qu_detail, url)
            pass
        while self.worker_pool._work_queue.qsize() != 0:
            time.sleep(1)
        time.sleep(3)


if __name__ == '__main__':
    app = Main()
    app.run()
