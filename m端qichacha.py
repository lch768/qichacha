#coding=utf-8
import threadpool,sys,json,collections,arrow
import time
reload(sys)
sys.setdefaultencoding('utf8')
from IpProxyInterface import IpProxy
from pymongo import MongoClient
def getNoHtmlBody(content):
    body = None
    try:
        dr = re.compile(r'<[^>]+>', re.S)
        body = dr.sub('', content)
    except Exception, ex:
        print (ex.message)
    return body
from datetime import datetime
client2 = MongoClient('')
db2 = client2.hanwenyuan_1

accountx1 = db2["hbaseGeti_ok_4"]
accountx2 = db2["qccInfo0531"]
#account1 = db2["qccGengxin1"]
import requests,re
ipProxy = IpProxy()

import logging
from logging.handlers import RotatingFileHandler
#str_fmt = '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
logger = logging.getLogger()
logger.setLevel(logging.INFO)
filehandler = logging.handlers.TimedRotatingFileHandler(
    filename="./qcc差集个体.log", when='D', interval=1, backupCount=5, encoding='utf-8'
)
# fmt = logging.Formatter(str_fmt)
# filehandler.setFormatter(fmt)
logger.addHandler(filehandler)
def spider(data):
    try:
        n1 = str(data["companyName"]).decode('utf-8')
        url = 'https://m.qichacha.com/search?key={}'.format(data["companyName"])
        ip1 = json.dumps(ipProxy.getRandomIP())
        ip = str(ip1).replace('"', '')
        proxies = {
            "http": "http://%s" % (ip),
            "https": "https://%s" % (ip),
        }
        # data = {"id":"22822","ps":2000,"pn":"1","int_cls":-100,"status":-100,"category":-100,"app_year":-100}
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "zh-CN,zh;q=0.9",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "referer": "https://www.qichacha.com",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Mobile Safari/537.36",
        }
        s = requests.session()
        c = s.get(url='https://m.qichacha.com/', headers=headers, proxies=proxies,timeout=5)
        cookies = requests.utils.dict_from_cookiejar(c.cookies)
        con1 = s.get(url=url, headers=headers,cookies=cookies, proxies=proxies,timeout=5)
        if con1.status_code == 405:
            logging.info('cooke过期。。。')
        else:
            id_list = re.findall('class="text-danger">(.*?)</div> <a href="(.*?)" class="a-decoration">',con1.content.replace('\r', '').replace('\t', '').replace('\n', ''))
            id = id_list[0][1]
            con = s.get(url='https://m.qichacha.com{}'.format(id), headers=headers, cookies=cookies, proxies=proxies,timeout=5)

            if con.status_code != 200:
                a = 1
                logging.info('%s,请求有错误'%(data["companyName"]))
                #accountx1.remove({"_id": data["id"]})
            else:
                if n1 in con.content:
                    info = re.findall('class="company-name">(.*?)<span',
                                      con.content.replace('\r', '').replace('\t', '').replace('\n', ''))
                    if len(info) > 0:
                        data1 = collections.OrderedDict()
                        CompanyBaseInfo = collections.OrderedDict()
                        data1["CompanyBaseInfo"] = CompanyBaseInfo
                        companyName = getNoHtmlBody(str(info[0]).replace(' ', ''))
                        if '基本' in companyName or '股东' in companyName or '成员' in companyName:
                            logging.info('%s,有问题'%(data["companyName"]))
                        else:
                            faRenName = re.findall('法人</div>(.*?)>(.*?)</div>',
                                                   con.content.replace('\r', '').replace('\t', '').replace('\n', ''))
                            gongShangZhuCeHao = re.findall('注册号</div>(.*?)>(.*?)</div>',
                                                           con.content.replace('\r', '').replace('\t', '').replace('\n', ''))
                            tongYiXinYongDaiMa = re.findall('统一社会信用代码</div>(.*?)>(.*?)</div>',
                                                            con.content.replace('\r', '').replace('\t', '').replace('\n', ''))
                            zhuCeZiBen1 = re.findall('注册资本</div>(.*?)>(.*?)</div>',
                                                     con.content.replace('\r', '').replace('\t', '').replace('\n', ''))
                            zhuCeTime = re.findall('成立日期</div>(.*?)>(.*?)</div>',
                                                   con.content.replace('\r', '').replace('\t', '').replace('\n', ''))
                            companyType = re.findall('企业类型</div>(.*?)>(.*?)</div>',
                                                     con.content.replace('\r', '').replace('\t', '').replace('\n', ''))
                            jingYinFanWei = re.findall('经营范围</div>(.*?)>(.*?)</div>',
                                                       con.content.replace('\r', '').replace('\t', '').replace('\n', ''))
                            zhuCeDiZhi = re.findall('公司住所</div>(.*?)>(.*?)</div>',
                                                    con.content.replace('\r', '').replace('\t', '').replace('\n', ''))
                            yingYeQiXian = re.findall('营业期限</div>(.*?)>(.*?)</div>',
                                                      con.content.replace('\r', '').replace('\t', '').replace('\n', ''))
                            companyStatus = re.findall('企业状态</div>(.*?)>(.*?)</div>',
                                                       con.content.replace('\r', '').replace('\t', '').replace('\n', ''))
                            CompanyBaseInfo["companyName"] = companyName
                            try:
                                CompanyBaseInfo["faRenName"] = getNoHtmlBody(faRenName[0][1].replace(' ', ''))
                            except:
                                CompanyBaseInfo["faRenName"] = None
                            try:
                                CompanyBaseInfo["gongShangZhuCeHao"] = getNoHtmlBody(gongShangZhuCeHao[0][1].replace(' ', ''))
                            except:
                                CompanyBaseInfo["gongShangZhuCeHao"] = None
                            try:
                                CompanyBaseInfo["tongYiXinYongDaiMa"] = getNoHtmlBody(tongYiXinYongDaiMa[0][1].replace(' ', ''))
                            except:
                                CompanyBaseInfo["tongYiXinYongDaiMa"] = None
                            try:
                                CompanyBaseInfo["zhuCeTime"] = (arrow.get(zhuCeTime[0][1].replace(' ', ''), 'YYYY-MM-DD',
                                                                          tzinfo='local').timestamp) * 1000
                            except:
                                CompanyBaseInfo["zhuCeTime"] = None
                            try:
                                CompanyBaseInfo["companyType"] = getNoHtmlBody(companyType[0][1].replace(' ', ''))
                            except:
                                CompanyBaseInfo["companyType"] = None
                            try:
                                CompanyBaseInfo["jingYinFanWei"] = getNoHtmlBody(jingYinFanWei[0][1].replace(' ', ''))
                            except:
                                CompanyBaseInfo["jingYinFanWei"] = None
                            try:
                                CompanyBaseInfo["zhuCeDiZhi"] = getNoHtmlBody(zhuCeDiZhi[0][1].replace(' ', ''))
                            except:
                                CompanyBaseInfo["zhuCeDiZhi"] = None
                            try:
                                CompanyBaseInfo["yingYeQiXian"] = yingYeQiXian[0][1].replace(' ', '')
                            except:
                                CompanyBaseInfo["yingYeQiXian"] = None
                            try:
                                CompanyBaseInfo["companyStatus"] = getNoHtmlBody(companyStatus[0][1].replace(' ', ''))
                            except:
                                CompanyBaseInfo["companyStatus"] = None
                            if len(zhuCeZiBen1) != 0:
                                zhuCeZiBen = zhuCeZiBen1[0][1].replace(' ', '')
                            else:
                                zhuCeZiBen = None
                            try:
                                c = re.sub(r'[^\x00-\x7F]+', ' ', zhuCeZiBen)
                                if '.' in c:
                                    d = float(c)
                                else:
                                    d = int(c)
                                CompanyBaseInfo["zhuCeZiBen"] = d
                            except:
                                CompanyBaseInfo["zhuCeZiBen"] = -1
                            try:
                                if '美' in zhuCeZiBen:
                                    CompanyBaseInfo["currency"] = '美元'
                                elif '港' in zhuCeZiBen:
                                    CompanyBaseInfo["currency"] = '港币'
                                else:
                                    CompanyBaseInfo["currency"] = '人民币'
                            except:
                                CompanyBaseInfo["currency"] = None
                            try:
                                if '万' in zhuCeZiBen:
                                    CompanyBaseInfo["unit"] = '万元'
                                else:
                                    CompanyBaseInfo["unit"] = '元'
                            except:
                                CompanyBaseInfo["unit"] = None
                            if CompanyBaseInfo['zhuCeZiBen'] == -1 or CompanyBaseInfo['unit'] == None or CompanyBaseInfo[
                                "currency"] == None:
                                CompanyBaseInfo["amount"] = None
                            else:
                                if CompanyBaseInfo['unit'] == '万元' and '人民币' in CompanyBaseInfo["currency"]:
                                    amount1 = CompanyBaseInfo["zhuCeZiBen"] * 10000
                                    amount2 = ("%.2f%%" % (amount1))
                                    CompanyBaseInfo["amount"] = (str(amount2).split('%'))[0]
                                elif CompanyBaseInfo['unit'] == '元' and '人民币' in CompanyBaseInfo["currency"]:
                                    amount1 = CompanyBaseInfo["zhuCeZiBen"]
                                    amount2 = ("%.2f%%" % (amount1))
                                    CompanyBaseInfo["amount"] = (str(amount2).split('%'))[0]
                                elif '万' in CompanyBaseInfo['unit'] and '美元' in CompanyBaseInfo["currency"]:
                                    amount1 = CompanyBaseInfo["zhuCeZiBen"] * 10000 * (6.3468)
                                    amount2 = ("%.2f%%" % (amount1))
                                    CompanyBaseInfo["amount"] = (str(amount2).split('%'))[0]
                                elif CompanyBaseInfo['unit'] == '元' and '美元' in CompanyBaseInfo["currency"]:
                                    amount1 = int(CompanyBaseInfo["zhuCeZiBen"]) * (6.3468)
                                    amount2 = ("%.2f%%" % (amount1))
                                    CompanyBaseInfo["amount"] = (str(amount2).split('%'))[0]
                                else:
                                    CompanyBaseInfo["amount"] = (CompanyBaseInfo["zhuCeZiBen"])
                            try:
                                if CompanyBaseInfo["tongYiXinYongDaiMa"] != '' and CompanyBaseInfo["tongYiXinYongDaiMa"] != '未公开' and \
                                        CompanyBaseInfo["tongYiXinYongDaiMa"] != None:
                                    tongyi_haoma1 = CompanyBaseInfo["tongYiXinYongDaiMa"]
                                    tongyi_haoma = tongyi_haoma1[2:8]
                                    CompanyBaseInfo["shengFen"] = tongyi_haoma[0:2]
                                    CompanyBaseInfo["city"] = tongyi_haoma[2:4]
                                    CompanyBaseInfo["district"] = tongyi_haoma[4:6]
                                elif CompanyBaseInfo["gongShangZhuCeHao"] != '' and CompanyBaseInfo["gongShangZhuCeHao"] != '未公开' and \
                                        CompanyBaseInfo["gongShangZhuCeHao"] != None:
                                    gongshang_haoma1 = CompanyBaseInfo["gongShangZhuCeHao"]
                                    gongshang_haoma = gongshang_haoma1[0:6]
                                    CompanyBaseInfo["shengFen"] = gongshang_haoma[0:2]
                                    CompanyBaseInfo["city"] = gongshang_haoma[2:4]
                                    CompanyBaseInfo["district"] = gongshang_haoma[4:6]
                                else:
                                    CompanyBaseInfo["shengFen"] = None
                                    CompanyBaseInfo["city"] = None
                                    CompanyBaseInfo["district"] = None

                            except:
                                CompanyBaseInfo["shengFen"] = None
                                CompanyBaseInfo["city"] = None
                                CompanyBaseInfo["district"] = None
                            # 股东信息
                            GuDongInfo_list = re.findall(
                                'class="stock-title"> <span >(.*?)>(.*?)<(.*?)class="company-status status-normal">(.*?)</span>(.*?)class="stock-text">(.*?)</div>(.*?)class="stock-text">(.*?)</div>(.*?)class="stock-text">(.*?)<(.*?)class="stock-text">(.*?)</div>',
                                con.content.replace('\r', '').replace('\t', '').replace('\n', ''))
                            if len(GuDongInfo_list) > 0:
                                gudong_list = []
                                for GuDongInfo in GuDongInfo_list:
                                    GuDongInfo_info = collections.OrderedDict()
                                    if len(str(GuDongInfo).decode('utf-8')) > 4:
                                        GuDongInfo_info["guDongCompanyName"] = getNoHtmlBody(str(GuDongInfo[1]))
                                        GuDongInfo_info["guDongName"] = None
                                    else:
                                        GuDongInfo_info["guDongCompanyName"] = None
                                        GuDongInfo_info["guDongName"] = getNoHtmlBody(str(GuDongInfo[1]))
                                    try:
                                        GuDongInfo_info["chuZiBiLi"] = getNoHtmlBody(str(GuDongInfo[5]))
                                    except:
                                        GuDongInfo_info["chuZiBiLi"] = None
                                    try:
                                        GuDongInfo_info["guDongLeiXing"] = getNoHtmlBody(str(GuDongInfo[7]))
                                    except:
                                        GuDongInfo_info["guDongLeiXing"] = None
                                    try:
                                        GuDongInfo_info["renJiaoMoney"] = getNoHtmlBody(str(GuDongInfo[9]).replace(' ', ''))
                                    except:
                                        GuDongInfo_info["renJiaoMoney"] = None
                                    try:
                                        GuDongInfo_info["renJiaoTime"] = (arrow.get(
                                            str(getNoHtmlBody(str(GuDongInfo[11]).replace(' ', ''))), 'YYYY-MM-DD',
                                            tzinfo='local').timestamp) * 1000
                                    except:
                                        GuDongInfo_info["renJiaoTime"] = None
                                    gudong_list.append(GuDongInfo_info)
                                data1["GuDongInfo"] = gudong_list

                            # 主要成员
                            ZhuYaoChengYuan_list = re.findall('class="employee-name">(.*?)</div> <div class="employee-job">(.*?)</div>',
                                                              con.content.replace('\r', '').replace('\t', '').replace('\n', ''))
                            if len(ZhuYaoChengYuan_list) > 0:
                                chengyuan_list = []
                                for chengyuan in ZhuYaoChengYuan_list:
                                    ZhuYaoChengYuan = collections.OrderedDict()
                                    ZhuYaoChengYuan["chengYuanName"] = getNoHtmlBody(str(chengyuan[0]))
                                    ZhuYaoChengYuan["chengYuanZhiWei"] = getNoHtmlBody(str(chengyuan[1]))
                                    chengyuan_list.append(ZhuYaoChengYuan)
                                data1["ZhuYaoChengYuan"] = chengyuan_list

                            nowTime = datetime.now().strftime('%Y-%m-%d')
                            logging.info('时间:%s,企业:%s'%(nowTime,CompanyBaseInfo["companyName"]))
                            accountx2.insert(data1)
                            accountx1.remove({"_id":data["id"]})
                    else:
                        logging.info('%s,有毛病' % (url))

                else:
                    logging.info('%s,有毛病'%(url))

    except Exception,e:
        repr(e)
        logging.info('%s,报错：。。%s'%(n1,e))
pool1 = threadpool.ThreadPool(15)

def start_pool(request):
    [pool1.putRequest(req) for req in request]
    pool1.wait()

if __name__ == "__main__":
    while True:
        try:
            k = accountx1.find().count()
            if k>0:
                x_list = []

                for conn in accountx1.find():
                    data3 = {}
                    data3["companyName"] = conn["companyName"]
                    # data3["shangji_name"] = conn["shangji_name"]
                    # data3["mother_name"] = conn["mother_name"]
                    if len(str(conn["companyName"]).decode('utf-8'))<=3:
                        accountx1.remove({"_id":conn["_id"]})
                    else:

                        data3["id"] = conn["_id"]

                        x_list.append(data3)
                        if len(x_list) == 1000:
                            request = threadpool.makeRequests(spider, x_list)
                            start_pool(request)
                            x_list = []
                if x_list != []:
                    request = threadpool.makeRequests(spider, x_list)
                    start_pool(request)
                    x_list = []
        except:
            pass