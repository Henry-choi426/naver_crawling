# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exporters import JsonItemExporter, CsvItemExporter
import csv

# class NaverCrawlerPipeline(object):
#     def __init__(self):
#         self.csvwriter = csv.writer(open("QnA_crawling.csv","w"))
#         self.csvwriter.writerow(["title","question_context","answer_text"])

#     def process_item(self, item, spider):
#         row = []
#         row.append(item["title"])
#         row.append(item["question_context"])
#         row.append(item["answer_text"])
#         self.csvwriter.writerow(row)
#         return item


class CsvPipeline(object):
    def __init__(self):
        self.file = open("./save_data/annoy_all_doc1.csv", 'wb')
        self.exporter = CsvItemExporter(self.file, encoding='UTF-8')
        self.exporter.start_exporting()
 
    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()
 
    def process_item(self, item, spider):
        self.exporter.export_item(item)
#         print('저장 완료')
        return item

