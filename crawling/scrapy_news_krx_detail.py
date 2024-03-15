import os
import csv
import scrapy
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs


class MySpider(scrapy.Spider):

    # 엑셀 파일 존재 유무 확인 -> 없을 시 생성
    columns = ['isu_cd', 'news_dd', 'news_clss_nm', 'lvl_val', 'title', 'url', 'contents']
    
    if not "krx_news.csv" in os.listdir() :
        with open('./krx_news.csv', 'w', encoding = 'CP949') as f : csv.writer(f).writerows([columns])

    if not "news_krx_bad_urls.csv" in os.listdir() :
        with open('./news_krx_bad_urls.csv', 'w', encoding = 'CP949') as f : csv.writer(f).writerows([columns])

    news_infos = pd.read_csv('./news_krx_urls.csv', encoding = 'cp949').dropna().to_dict(orient='records')

    # 크롤링을 시작할 URL 목록
    headers = {'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'}
    url_infos = list(zip(list(range(len(news_infos))), [x['url'] for x in news_infos]))
    
    num_total = len(news_infos)

    # scrapy 시작!
    name = "Null"

    # 각 언론사별 뉴스 내용 클롤링 함수
    def news_text_crawler(self, r) :
        
        t = None

        if bs(r.text, 'html.parser').find('div', {'id' : 'articleBody'}) : 
            t = bs(r.text, 'html.parser').find('div', {'id' : 'articleBody'}).text.strip()
        elif bs(r.text, 'html.parser').find('article', {'id' : 'article-view-content-div'}) :
            t = bs(r.text, 'html.parser').find('article', {'id' : 'article-view-content-div'}).text.strip()
        elif bs(r.text, 'html.parser').find('div', {'class' : 'article_content'}) :
            t = bs(r.text, 'html.parser').find('div', {'class' : 'article_content'}).text.strip()
        elif bs(r.text, 'html.parser').find('div', {'class' : 'box'}) :
            t = bs(r.text, 'html.parser').find('div', {'class' : 'box'}).text.strip()
        elif  bs(r.text, 'html.parser').find('div', {'itemprop' : 'articleBody'}) :       
            t = bs(r.text, 'html.parser').find('div', {'itemprop' : 'articleBody'}).text.strip()
        elif  bs(r.text, 'html.parser').find('div', {'class' : 'article_view'}) :
            t = bs(r.text, 'html.parser').find('div', {'class' : 'article_view'}).text.strip()
        elif bs(r.text, 'html.parser').find('div', {'class' : 'article_body'}) :    
            t = bs(r.text, 'html.parser').find('div', {'class' : 'article_body'}).text.strip()
        elif bs(r.text, 'html.parser').find('div', {'class' : 'articleView'}) :   
            t = bs(r.text, 'html.parser').find('div', {'class' : 'articleView'}).text.strip()
        elif bs(r.text, 'html.parser').find('div', {'class' : 'article_area'}) :   
            t = bs(r.text, 'html.parser').find('div', {'class' : 'article_area'}).text.strip()
        elif bs(r.text, 'html.parser').find('div', {'id' : 'wrapCont'}) :        
            t = bs(r.text, 'html.parser').find('div', {'id' : 'wrapCont'}).text.strip()
        elif bs(r.text, 'html.parser').find('div', {'class' : 'article_con'}) :      
            t = bs(r.text, 'html.parser').find('div', {'class' : 'article_con'}).text.strip()
        elif  bs(r.text, 'html.parser').find('div', {'class' : 'article-body'}) :      
            t = bs(r.text, 'html.parser').find('div', {'class' : 'article-body'}).text.strip()
        elif bs(r.text, 'html.parser').find('div', {'id' : 'article_content'}) : 
            t = bs(r.text, 'html.parser').find('div', {'id' : 'article_content'}).text.strip()
        elif  bs(r.text, 'html.parser').find('div', {'id' : 'article-view-content-div'}) :   
            t = bs(r.text, 'html.parser').find('div', {'id' : 'article-view-content-div'}).text.strip()
        elif bs(r.text, 'html.parser').find('td', {'class' : 'td_sub_read_contents'}) : 
            t = bs(r.text, 'html.parser').find('td', {'class' : 'td_sub_read_contents'}).text.strip()
        elif bs(r.text, 'html.parser').find('div', {'class' : 'content'}) : 
            t = bs(r.text, 'html.parser').find('div', {'class' : 'content'}).text.strip()
        elif bs(r.text, 'html.parser').find("div", {'class' : 'text'}) :    
            t = bs(r.text, 'html.parser').find("div", {'class' : 'text'}).text.strip()
        elif bs(r.text, 'html.parser').find('article', {'itemprop' : "articleBody"}) :    
            t = bs(r.text, 'html.parser').find('article', {'itemprop' : "articleBody"}).text.strip()
        elif bs(r.text, 'html.parser').find('div', {'id' : "cont_newstext"}) :
            t = bs(r.text, 'html.parser').find('div', {'id' : "cont_newstext"}).text.strip()
        elif bs(r.text, 'html.parser').find('div', {'class' : 'text_area'}) : 
            t = bs(r.text, 'html.parser').find('div', {'class' : 'text_area'}).text.strip()
        elif bs(r.text, 'html.parser').find('div', {'class' : 'article_cont_wrap'}) :    
            t = bs(r.text, 'html.parser').find('div', {'class' : 'article_cont_wrap'}).text.strip()
        elif bs(r.text, 'html.parser').find('div', {'class' : 'articleArea'}) :
            t = bs(r.text, 'html.parser').find('div', {'class' : 'articleArea'}).text.strip()
        elif  bs(r.text, 'html.parser').find('div', {'class' : 'view-cont'}) :
            t = bs(r.text, 'html.parser').find('div', {'class' : 'view-cont'}).text.strip()
        elif bs(r.text, 'html.parser').find('div', {'id' : 'content'}) :
            t = bs(r.text, 'html.parser').find('div', {'id' : 'content'}).text.strip()
        elif bs(r.text, 'html.parser').find('div', {'class' : 'article_txt_container'}) :
            t = bs(r.text, 'html.parser').find('div', {'class' : 'article_txt_container'}).text.strip()
        elif bs(r.text, 'html.parser').find('span', {'class' : 'news_text'}) :
            t = bs(r.text, 'html.parser').find('span', {'class' : 'news_text'}).text.strip()
        elif bs(r.text, 'html.parser').find('div', {'class' : 'article'}) :
            t = bs(r.text, 'html.parser').find('div', {'class' : 'article'}).text.strip()
        elif bs(r.text, 'html.parser').find('div', {'class' : 'cont_view'}) :
            t = bs(r.text, 'html.parser').find('div', {'class' : 'cont_view'}).text.strip()
        elif bs(r.text, 'html.parser').find('div', {'id' : 'body_wrap'}) :
            t = bs(r.text, 'html.parser').find('div', {'id' : 'body_wrap'}).text.strip()
        elif bs(r.text, 'html.parser').find('div', {'id' : 'articleContent'}) :
            t = bs(r.text, 'html.parser').find('div', {'id' : 'articleContent'}).text.strip()
            
        # 인코딩 변환 필요 사이트    
        elif "nownews.seoul" in bs(r.text, 'html.parser').url and bs(r.text, 'html.parser').find('div', {'id' : 'articleContent'}) :
            r_ = requests.get(bs(r.text, 'html.parser').url, headers = self.headers)
            r_.encoding = 'utf-8'
            t = bs(r_.text, 'html.parser').find('div', {'id' : 'articleContent'}).text.strip()

        elif "kookje.co" in bs(r.text, 'html.parser').url and bs(r.text, 'html.parser').find('div', {'class' : 'news_article'}) :
            r_ = requests.get(bs(r.text, 'html.parser').url, headers = self.headers)
            r_.encoding = 'euc-kr'
            t = bs(r_.text, 'html.parser').find('div', {'class' : 'news_article'}).text.strip()
            
        elif "mt.co.kr" in requests.get(bs(r.text, 'html.parser').url, headers = self.headers).text.split('"')[1] :
            u_ = requests.get(r.url, headers = self.headers).text.split('"')[1]
            t = bs(requests.get('https:' + requests.get(bs(r.text, 'html.parser').url, headers = self.headers).text.split('"')[1]).text, 'html.parser').find('div', {'id' : 'textBody'}).text.strip()
        
        elif "ihalla.com" in bs(r.text, 'html.parser').url and bs(r.text, 'html.parser').find('div', {'class' : 'article_txt'}) :
            r_ = requests.get(bs(r.text, 'html.parser').url, headers = self.headers)
            r_.encoding = 'utf-8'
            t = bs(r_.text, 'html.parser').find('div', {'class' : 'article_txt'}).text.strip()
            
        # # 지워진 뉴스들...        
        # elif '존재하지 않는' in requests.get(bs(r.text, 'html.parser').url, headers = self.headers).text : t = 'deleted_news'
        # elif '기사가 없습니다' in requests.get(bs(r.text, 'html.parser').url, headers = self.headers).text : t = 'deleted_news'
        # elif '찾을 수 없습니다' in requests.get(bs(r.text, 'html.parser').url, headers = self.headers).text : t = 'deleted_news'
        # elif '/www.yeongnam.com/web/err.php' in requests.get(bs(r.text, 'html.parser').url, headers = self.headers).text : t = 'deleted_news'
        # elif 'link rel="shortcut icon" href="https://img.seoul.co.kr/seoul.ico' in requests.get(bs(r.text, 'html.parser').url, headers = self.headers).text : t = 'deleted_news'
        # elif 'ytn.co' in r.url and'href="/img/comm/favicon.ico" type="image/x-icon' in requests.get(bs(r.text, 'html.parser').url, headers = self.headers).text : t = 'deleted_news'
        # elif 'dt.co' in r.url and'http://img.dt.co.kr/images/img_nop01.gif' in requests.get(bs(r.text, 'html.parser').url, headers = self.headers).text : t = 'deleted_news'
            
            
            
        # 지워진 뉴스들...        
        elif '존재하지 않는' in requests.get(r.url, headers = self.headers).text : t = 'deleted_news'
        elif '기사가 없습니다' in requests.get(r.url, headers = self.headers).text : t = 'deleted_news'
        elif '찾을 수 없습니다' in requests.get(r.url, headers = self.headers).text : t = 'deleted_news'
        elif '/www.yeongnam.com/web/err.php' in requests.get(r.url, headers = self.headers).text : t = 'deleted_news'
        elif 'link rel="shortcut icon" href="https://img.seoul.co.kr/seoul.ico' in requests.get(r.url, headers = self.headers).text : t = 'deleted_news'
        elif 'ytn.co' in r.url and'href="/img/comm/favicon.ico" type="image/x-icon' in requests.get(r.url, headers = self.headers).text : t = 'deleted_news'
        elif 'dt.co' in r.url and'http://img.dt.co.kr/images/img_nop01.gif' in requests.get(r.url, headers = self.headers).text : t = 'deleted_news'
            
            
        else : pass
        
        return t


    # 처음 시작 명령 보내는 함수
    def start_requests(self):
        
        for idx, url in self.url_infos :
            
            if not url.startswith('http') : url = 'https://' + url
            
            yield scrapy.Request(url, headers=self.headers, callback = self.parse_start, cb_kwargs = {'idx' : idx})
            
    # 각 URL에서 데이터 추출 및 처리
    def parse_start(self, response, idx):
        
        info_tem = self.news_infos[idx]
        
        text = self.news_text_crawler(response)
        if text : 
            info_tem['contents'] = text
            file_name = 'krx_news'
            
        else : file_name = 'news_krx_bad_urls'
                        
        with open(f'./{file_name}.csv', 'a', newline = '\n', encoding = 'CP949') as f :
                        csv.DictWriter(f, fieldnames = self.columns).writerows([info_tem]) 

        if idx % 10 == 0 : print(f'status : {idx} / {self.num_total}, {round((idx/self.num_total)*100, 2)}%  /// now : {info_tem["title"]}')