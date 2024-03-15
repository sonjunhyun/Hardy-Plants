import os
import csv
import scrapy
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup as bs


class MySpider(scrapy.Spider):
    
    headers = {
    'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'
    }

    # 엑셀 파일 존재 유무 확인 -> 없을 시 생성
    columns = ['press', 'writer', 'date', 'company', 'title', 'contents', 'url']
    
    if not "news.csv" in os.listdir() :
        with open('./news.csv', 'w', encoding = 'cp949') as f : 
            csv.writer(f).writerows([columns])

    df = pd.read_csv('./url_list.csv', encoding = 'cp949')
    len_df = len(df)
    
    # scrapy 시작!
    name = "Null"

 
    # 처음 시작 명령 보내는 함수
    def start_requests(self) :

        # for i in tqdm(range(len(self.df))) :
        for i in range(len(self.df)) :

            press, company, url = self.df.iloc[i, :]

            if url.startswith('https://n.news') : 
                yield scrapy.Request(url, headers = self.headers, callback = self.parse, cb_kwargs = {'company' : company, 'press' : press, 'i' : i})


    def parse(self, response, company, press, i) : 
        
        s = bs(response.text, 'html.parser')

        infos = {
            "press" : press,
            "date" : s.find('span', {'class' : '_ARTICLE_DATE_TIME'}).text.strip(),
            'company' : company,
            "title" : s.find('h2', {'id' : 'title_area'}).text.strip(),
            "contents" : s.find('article', {'id' : 'dic_area'}).text.strip(),
            "url" : response.url
        }            
        
        tit = infos['title']

        try : infos['writer'] = s.find('em', {'class' : 'media_end_head_journalist_name'}).text
        except : pass
              
        with open(f'./news.csv', 'a', newline = '\n', encoding = 'CP949') as f :
            csv.DictWriter(f, fieldnames = self.columns).writerows([infos])

        if i % 10 == 0 : print(f'turn : {i} / {self.len_df} ({round((i/self.len_df)*100, 2)}%) , title : {tit}')