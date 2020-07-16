import re
import time

import requests
from lxml import etree


def test(url):
    ret_list = []
    city = re.findall(r'https://(.*?).ke', url)[0]
    qu = re.findall(r'chengjiao/(.*?)/', url)[0]
    for i in range(100):
        t_url = url + '/pg{}/'.format(i + 1)
        resp = requests.get(t_url).text
        fang_url_list = etree.HTML(resp).xpath('//ul[@class="listContent"]/li')
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
            item['guapaijia'] = li.xpath('.//span[@class="dealCycleTxt"]/span[1]/text()')[0].replace('挂牌', '').replace(
                '万', '')
            item['zhouqi'] = li.xpath('.//span[@class="dealCycleTxt"]/span[2]/text()')[0].replace('成交周期', '').replace(
                '天', '')
            item['jiangjia'] = (int(item['guapaijia']) - int(item['dealPrice'])) / int(item['guapaijia'])
            item['url'] = li.xpath('.//a[@class="CLICKDATA maidian-detail"]/@href')[0]
            item['crawl_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            item['caoxiang'] = li.xpath('.//div[@class="houseInfo"]/text()')[1].split('|')[0].strip()
            item['zhuangxiu'] = li.xpath('.//div[@class="houseInfo"]/text()')[1].split('|')[1].strip()
            print(item)
            ret_list.append(item)


if __name__ == '__main__':
    test('https://hf.ke.com/chengjiao/baohe/')
