# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exporters import JsonItemExporter, CsvItemExporter
import csv


class CsvPipeline(object):
    def __init__(self):
        self.file = open("./save_data/blog_data.csv", 'wb')
        self.exporter = CsvItemExporter(self.file, encoding='UTF-8')
        self.exporter.start_exporting()
 
    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()
 
    def process_item(self, item, spider):
        self.exporter.export_item(item)
#         print('저장 완료')
        return item
