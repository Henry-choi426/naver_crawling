import scrapy
from scrapy.spiders import Spider
import re
from blog_cralwer.items import NaverCrawlerItem
from scrapy.http import Request
from scrapy.selector import Selector
from bs4 import BeautifulSoup
import datetime
import calendar

#https://section.blog.naver.com/BlogHome.naver?keyword=%23%EC%98%A4%EB%8A%98%EC%9D%BC%EA%B8%B0%20%23%ED%96%89%EB%B3%B5&startDate=2022-04-07&endDate=2022-04-07

#https://section.blog.naver.com/Search/Post.naver?pageNo=1&rangeType=ALL&orderBy=sim&keyword=%23오늘일기%20%23행복
#https://section.blog.naver.com/Search/Post.naver?pageNo=1&rangeType=PERIOD&orderBy=sim&startDate=2022-04-12&endDate=2022-04-12&keyword=%23%EC%98%A4%EB%8A%98%EC%9D%BC%EA%B8%B0%20%23%ED%96%89%EB%B3%B5

class NaverBlogSpider(scrapy.Spider):
    name = 'naver_blog'

    def start_requests(self):
        years = [i for i in range(2022,2023)]
        keywords = '%23오늘일기%20%23행복'
        
        end_day_flag = False
        for year in years:
            start_day = datetime.datetime(year,1,1)
            if datetime.datetime(year,12,calendar.monthrange(year,12)[1]) > datetime.datetime.today():
                end_day = datetime.datetime.today()
                end_day_flag = True
            else:
                end_day = datetime.datetime(year,12,31)
                

            while start_day <= end_day:
                start = start_day.strftime('%Y-%m-%d')
                
                yield scrapy.Request(f"https://section.blog.naver.com/Search/Post.naver?pageNo=1&rangeType=PERIOD&orderBy=sim&startDate={start}&endDate={start}&keyword={keywords}",self.parse1, 
                meta = {'start_month' : start, 'end_month' : start, 'keywords' : keywords})
                start_day = start_day + datetime.timedelta(days=1)
                
            if end_day_flag:
                break
                #content > section > div.category_search > div.search_information > span > span > em
                
                
    def parse1(self, response):
        try:
            page_limit = response.xpath('//*[@id="content"]/section/div[1]/div[2]/span/span/em').extract()
#             page_limit = response.xpath('//*[@id="content"]/section/div[1]/div[2]/span/span/em').extract_first()

            print(page_limit)
            print(response)
        
            if not page_limit:
                print(response.meta['start_month']," 기간에 글이 없습니다.")
                return
            page_limit = re.sub('(<([^>]+)>)', "", page_limit)
            page_limit = re.sub(',', "", page_limit)
            page_limit = int(re.sub('1-\d*/', "", page_limit))
            print(response.meta['start_month'],"일 게시물 수:", page_limit)
            page_limit = min((page_limit // 7) + 1, 101)
    
            for page in range(1, page_limit+1):
            
                yield scrapy.Request(f"https://section.blog.naver.com/Search/Post.naver?pageNo={page}&rangeType=PERIOD&orderBy=sim&startDate={response.meta['start_month']}&endDate={response.meta['end_month']}&keyword={response.meta['keywords']}",self.parse2)
            
        except Exception as e:
            print("페이지 수 확인 에러, 코드:",e)
            
       #content > section > div.area_list_search > div:nth-child(1) 
    #content > section > div.area_list_search > div:nth-child(2)
    ##content > section > div.area_list_search > div:nth-child(1)
    # 3. 해당 페이지의 QnA links 크롤링
    def parse2(self, response):
        try:
            qna_links = []
            temp_link = ''
            temp_link = response.css('#content > section > div.area_list_search > div:nth-child')
            print(temp_link)
            qna_links = temp_link.css('dl > dt > a::attr(post-url)').getall()
            # print("이번 페이지에서 크롤링할 게시물 수:",len(qna_links))

            for index, qna_link in enumerate(qna_links):
                yield scrapy.Request(url=qna_link, callback= self.parse3)
                # print("이번 게시물 번호:",index)

        except Exception as e:
            print("-"*30,"Link 불러오기 오류, 코드:",e,"-"*30)