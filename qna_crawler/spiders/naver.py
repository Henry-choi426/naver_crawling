import scrapy
from scrapy.spiders import Spider
import re
from naver_crawler.items import NaverCrawlerItem
from scrapy.http import Request
from scrapy.selector import Selector
from bs4 import BeautifulSoup
import datetime
import calendar

class QuotesSpider(scrapy.Spider):
    
    '''
    네이버 지식인 크롤러
    
    # 수집 데이터
        - 질문 제목, 질문 내용, 날짜, 태그, 답변
    
    # 크롤링 속도
        DOWNLOAD_DELAY = 0.1(초), CONCURRENT_REQUESTS(동시 요청 수) = 16(개) 기준 row당 약 0.13 ~ 0.14초 소요
        ex) 2000개 크롤링 시 소요시간: 약 280초
            100000개 크롤링 시 소요시간: 약 14000초 = 약 3.9시간
    
    # work flow
        1. 기간, 검색 키워드, 태그 설정
        2. 각 기간 단위로 페이지 수 확인
        3. 각 페이지의 QnA 링크 크롤링
        4. 각 QnA 링크 접속 후 상세 데이터 크롤링 및 csv파일 저장
    
    # 전처리 내용
        - html태그 및 다중 여백 제거
        - 이메일, URL 제거
        - 다중 answer의 구분자[sep] 및 의사 답변 구분자[doc] 추가
    
    # 추후 논의 내용
        - 이모티콘 전처리 -> 치환/제거 항목인지 체크
        - 질문자 채택 답변의 필요 여부 확인
    '''
    
    name = "NaverSpider"  #spider 이름

    # 1. 기간 및 키워드, 태그 설정
    def start_requests(self):
        # 기간 및 키워드, 태그 직접 설정하기 (한방신경정신과 = &dirId=70305 , 정신건강의학과 = &dirId=70109, 신경과 = &dirId=70108)
        years = [i for i in range(2002,2023)]
        keywords = '짜증'
#         keywords = '분통|성질|속상|부글부글|신경질|억울|열 받는|울화통|원망|적개심|핏대|환멸'
# 불안|막막|걱정|안절부절|초조


        # 일 단위 페이지 크롤링
        # 최대 오늘 날짜까지만 크롤링
        
        # 한방신경정신과 크롤링
#         dirId = "&dirId=70305" # 공백 시 전체 태그 크롤링
#         end_day_flag = False
#         for year in years:
#             start_day = datetime.datetime(year,1,1)
#             if datetime.datetime(year,12,calendar.monthrange(year,12)[1]) > datetime.datetime.today():
#                 end_day = datetime.datetime.today()
#                 end_day_flag = True
#             else:
#                 end_day = datetime.datetime(year,12,31)
                
#             while start_day <= end_day:
#                 start = start_day.strftime('%Y.%m.%d')
#                 yield scrapy.Request(f"https://kin.naver.com/search/list.nhn?sort=none&query={keywords}&period={start}.%7C{start}.&section=qna{dirId}",self.parse1, 
#                 meta = {'start_month' : start, 'end_month' : start, 'keywords' : keywords, 'dirId' : dirId})
#                 start_day = start_day + datetime.timedelta(days=1)
                
#             if end_day_flag:
#                 break
        
        # 정신건강의학과 크롤링
        end_day_flag = False
        dirId = "&dirId=70109" # 공백 시 전체 태그 크롤링
        for year in years:
            start_day = datetime.datetime(year,1,1)
            if datetime.datetime(year,12,calendar.monthrange(year,12)[1]) > datetime.datetime.today():
                end_day = datetime.datetime.today()
                end_day_flag = True
            else:
                end_day = datetime.datetime(year,12,31)
                
            while start_day <= end_day:
                start = start_day.strftime('%Y.%m.%d')
                yield scrapy.Request(f"https://kin.naver.com/search/list.nhn?sort=none&query={keywords}&period={start}.%7C{start}.&section=qna{dirId}",self.parse1, 
                meta = {'start_month' : start, 'end_month' : start, 'keywords' : keywords, 'dirId' : dirId})
                start_day = start_day + datetime.timedelta(days=1)
                
            if end_day_flag:
                break
                
        # 신경과 크롤링
        end_day_flag = False
        dirId = "&dirId=70108" # 공백 시 전체 태그 크롤링
        for year in years:
            start_day = datetime.datetime(year,1,1)
            if datetime.datetime(year,12,calendar.monthrange(year,12)[1]) > datetime.datetime.today():
                end_day = datetime.datetime.today()
                end_day_flag = True
            else:
                end_day = datetime.datetime(year,12,31)
                
            while start_day <= end_day:
                start = start_day.strftime('%Y.%m.%d')
                yield scrapy.Request(f"https://kin.naver.com/search/list.nhn?sort=none&query={keywords}&period={start}.%7C{start}.&section=qna{dirId}",self.parse1, 
                meta = {'start_month' : start, 'end_month' : start, 'keywords' : keywords, 'dirId' : dirId})
                start_day = start_day + datetime.timedelta(days=1)
                
            if end_day_flag:
                break
                
        
        # 월 단위 페이지 크롤링
#         for year in years:
#             for month in range(1,3):
#                 start_month = datetime.datetime(year,month,1).strftime('%Y.%m.%d')
#                 end_month = datetime.datetime(year,month,calendar.monthrange(year,month)[1]).strftime('%Y.%m.%d')

#                 if datetime.datetime(year,month,calendar.monthrange(year,month)[1]) > datetime.datetime.today():
#                     end_month = datetime.datetime.today().strftime('%Y.%m.%d')
#                     end_day_flag = True

#                 print("-"*30, start_month,' ~ ',end_month, "-"*30)
                
#                 yield scrapy.Request(f"https://kin.naver.com/search/list.nhn?sort=none&query={keywords}&period={start_month}.%7C{end_month}.&section=qna{dirId}",self.parse1, 
#                 meta = {'start_month' : start_month, 'end_month' : end_month, 'keywords' : keywords, 'dirId' : dirId})

#                 if end_day_flag:
#                     break
        
    # 2. 해당 월 페이지 수 확인
    # 한 기간 단위 당 최대 페이지수를 체크 -> 101페이지가 넘어가면 중복된 검색 내용 출력됨
    def parse1(self, response):
        try:
            page_limit = response.xpath('//*[@id="s_content"]/div[3]/h2/span/em').extract_first()
            if not page_limit:
#                 print(response.meta['start_month']," 기간에 글이 없습니다.")
                return

            page_limit = re.sub('(<([^>]+)>)', "", page_limit)
            page_limit = re.sub(',', "", page_limit)
            page_limit = int(re.sub('1-\d*/', "", page_limit))
#             print(response.meta['start_month'],"일 게시물 수:", page_limit)
            page_limit = min((page_limit // 10) + 1, 101)

            for page in range(1, page_limit+1):
                yield scrapy.Request(f"https://kin.naver.com/search/list.nhn?sort=none&query={response.meta['keywords']}&period={response.meta['start_month']}.%7C{response.meta['end_month']}.&section=qna{response.meta['dirId']}&page={page}",self.parse2)
        
        except Exception as e:
            print("페이지 수 확인 에러, 코드:",e)

    # 3. 해당 페이지의 QnA links 크롤링
    def parse2(self, response):
        try:
            qna_links = []
            temp_link = ''
            temp_link = response.css('#s_content > div.section > ul > li')
            qna_links = temp_link.css('dl > dt > a::attr(href)').getall()
            # print("이번 페이지에서 크롤링할 게시물 수:",len(qna_links))

            for index, qna_link in enumerate(qna_links):
                yield scrapy.Request(url=qna_link, callback= self.parse3)
                # print("이번 게시물 번호:",index)

        except Exception as e:
            print("-"*30,"Link 불러오기 오류, 코드:",e,"-"*30)

    # 4. QnA 크롤링
    def parse3(self, response):
        try:
            model = NaverCrawlerItem()
            q_title, q_context, date, tag, a_context,answer_cnt = ['','','','','','']
            qna_all = []

            # title, content, date, tag 크롤링
            # title에서 내공이 설정된 경우 예외처리, title을 작성하지 않을 경우 content가 title로 올라가서 저장된다.
            qna_all.append(response.xpath('//*[@id="content"]/div[1]/div/div[1]/div[2]/div/div').getall()[-1]) # title
            qna_all.append(response.xpath('//*[@id="content"]/div[1]/div/div[1]/div[3]').extract_first()) # content
            qna_all.append(response.xpath('//*[@id="content"]/div[1]/div/div[3]/div[1]/span[1]').extract_first()) # date
            qna_all.append(response.xpath('//*[@id="content"]/div[1]/div/div[2]/a/text()').extract_first()) # tag

            # Answer 전체 데이터 크롤링 및 전처리
            # 두가지 경우의 답변 xpath를 적용하여 삭제된 답변을 제외한 모든 답변을 크롤링
            answer_cnt = response.xpath('//*[@id="answerArea"]/div/div[1]/div[1]/h3/em/text()').extract_first()
            if answer_cnt:
                for number in range(1,int(answer_cnt)+1):
                    temp_answer = response.xpath(f'//*[@id="answer_{number}"]/div[2]/div[1]').extract_first()
                    if not temp_answer:
                        temp_answer = response.xpath(f'//*[@id="answer_{number}"]/div[2]/div/div/div/div/div/div/div').extract_first()
                        if not temp_answer:
                            continue
                    answer_writer = response.xpath(f'//*[@id="answer_{number}"]/div[1]/div[2]/div/p/a/text()').extract_first()
#                     answer_choice = response.xpath(f'//*[@id="answer_{number}"]/div[1]/div[2]/div[2]').extract_first() # 질문자 채택 확인 로직
                    
                    if temp_answer:
                        temp_answer = re.sub('(<([^>]+)>)', "", temp_answer)
#                         temp_answer = re.sub(" +", " ", temp_answer)
                        temp_answer = ' '.join(temp_answer.split()) # 다중 공백 제거, 속도 더 빠름
                        temp_answer = re.sub(u'\xa0', " ", temp_answer)
                        temp_answer = re.sub(u'\u200b', " ", temp_answer)
                        temp_answer = re.sub('([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)', '', temp_answer) # 이메일 제거
                        temp_answer = re.sub('(http|ftp|https)://(?:[-\w.]|(?:%[\da-fA-F]{2}))+', '', temp_answer) # URL 제거
                        temp_answer = temp_answer.strip()
                    else:
                        break
                    
                    # 의사 확인 로직 -> 닉네임에 특정 부분에 '의사' 키워드가 있는 경우 의사로 판단. 비공개 답변자의 경우 일반 답변으로 구분
                    # ex) OOO 한의사님 답면, OOO 의사님 답변 -> 의사가 아닌 일반 유저의 닉네임이 '~~~의사' 로 끝날 경우 오판단 할 경우가 있음.
                    if answer_writer:
                        if answer_writer[-6:-4] == '의사':
                            temp_answer = ' [doc] ' + temp_answer
                    if number == 1:
                        a_context = temp_answer # answer
                    else:
                        a_context += ' [sep] ' + temp_answer
    
            # answer 외 다른 데이터 전처리
            for i, v in enumerate(qna_all):
                if v:
                    v = re.sub('(<([^>]+)>)', "", v)
#                     v = re.sub(" +", " ", v)
                    v = ' '.join(v.split()) # 다중 공백 제거, 속도 더 빠름
                    v = re.sub(u'\xa0', " ", v)
                    v = re.sub(u'\u200b', " ", v)
                    v = re.sub('([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)', '', v) # 이메일 제거
                    v = re.sub('(http|ftp|https)://(?:[-\w.]|(?:%[\da-fA-F]{2}))+', '', v) # URL 제거
                    v = v.strip()
                    
                if i == 0:
                    q_title = v
                elif i == 1:
                    q_context = v
                elif i == 2:
                    # yyyy.mm.dd 및 오늘 작성한 글 전처리
                    if re.search('시간',v):
                        v = datetime.datetime.today().strftime('%Y.%m.%d')
                    else:
                        v = v[3:]
                    date = v
                elif i == 3:
                    tag = v[2:]
            
            model["title"] = q_title
            model["content"] = q_context
            model["date"] = date
            model["tag"] = tag
            model["answer"] = a_context
            
            yield model
        except Exception as e:
            print("-"*30,'크롤링 오류 칼럼:',i, '코드:',e,"-"*30)
            # print("해당 QnA:",qna_all)
