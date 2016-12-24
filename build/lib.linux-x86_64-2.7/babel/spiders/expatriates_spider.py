import scrapy
import pdb
from babel.items import *
from datetime import datetime 
import re
import unicodedata

# validate the value of html node
#   return string value, if data is validated
#   return "", otherwise
def validate(node):
  if len(node) > 0:
      value = node.extract_first().strip()
      return value
  else: 
      return ""

#convert date format
def correct_date(ori_date):
  if ori_date == "":
    return ""
  return datetime.strptime(ori_date, '%A, %B %d, %Y').strftime('%Y-%m-%d %H:%M:%S')

#Get Price from str
def extract_price(pri_str):
  return pri_str.split(" ", 1)[-1]

#Initialize BaseItem Class
def init_base_item(item):
  item["category_id"] = item["category"] = item["item_type"] = item["title"] = item["post_date"] = item["price"] = item["details"] = item["contact_name"] = item["contact_email"] = item["expiration_date"] = ""
  item["image_urls"] = item["image_names"] = item["images"] = [] 

class ExpatriatescarsSpider(scrapy.Spider):
  #name of the spider
  name = "expatriatescars"

  #list of allowed domains
  allowed_domains = ["www.expatriates.com"]
  
  #list of urls
  start_urls = [
    "http://www.expatriates.com/classifieds/uae/vehicles/",
  ]

  #domain url
  domain = "http://www.expatriates.com"

  def parse(self, response):
    total_page = 0
    if len(response.xpath('//nav[@class="pagination"]//a//text()').extract()) > 0:
      total_page = max(map((lambda x: int(x) if x.isdigit() else 0), response.xpath('//nav[@class="pagination"]//a//text()').extract()))

    if total_page == 0:
      url = "http://www.expatriates.com/classifieds/uae/vehicles/"
      request = scrapy.Request(url, callback=self.parse_each_page)
      yield request
    else:
      for page in range(1,total_page):
        if page == 1:
          url = "http://www.expatriates.com/classifieds/uae/vehicles/index.html"
        else: 
          url = "http://www.expatriates.com/classifieds/uae/vehicles/index%s.html" % (page-1)*100
        request = scrapy.Request(url, callback=self.parse_each_page)
        yield request

  def parse_each_page(self, response):
    for item in response.xpath('//a[contains(./@href, "cls")]'):
      item_detail_url = "http://www.expatriates.com" + validate(item.xpath('./@href'))
      request = scrapy.Request(item_detail_url, callback=self.parse_car_detail)
      yield request
  #parse car detail
  #get the detail info for each car
  def parse_car_detail(self, response):
    car = self.init_car()

    car["category"] = "Cars"
    car["item_type"] = "car"
    car["title"] = validate(response.xpath('//div[contains(@class,"page-title")]//text()[2]'))
    car["post_date"] = correct_date(validate(response.xpath('//li[contains(./strong/text(),"Date")]/text()')))
    car["make"] = validate(response.xpath('//div[@class="post-body"]//text()[2]')).split(":")[-1].strip().split(" ", 1)[0]
    car["model"] = validate(response.xpath('//div[@class="post-body"]//text()[2]')).split(":")[-1].strip().split(" ", 1)[-1]
    car["price"] = [int(s) for s in car["title"].split() if s.isdigit()][0]
    car["year"] = validate(response.xpath('//div[@class="post-body"]//text()[3]')).split(":")[-1].strip()
    car["body_style"] = "Wagon"
    car["seller"] = ""
    car["doors"] = ""
    car["color"] = ""
    car["mileage"] = validate(response.xpath('//div[@class="post-body"]//text()[5]')).split(":")[-1].strip()
    car["warranty"] = ""
    car["contact_email"] = ""
    car["expiration_date"] = ""
    car["region"] = response.xpath('//li[contains(./strong/text(),"Region")]/text()').extract_first().split(" ")[1]
    images = response.xpath('//div[@class="posting-images top-margin"]//img/@src').extract()
    car["image_urls"] =  map((lambda x: "http://www.expatriates.com%s" % x), images)
    car["details"] = " ".join(response.xpath('//div[@class="post-body"]//text()').extract()).strip()
    car["contact_number"] = ""
    if len(response.xpath("//button[contains(./@class,'posting-phone')]//text()").extract()) >=2: 
      car["contact_number"] = response.xpath("//button[contains(./@class,'posting-phone')]//text()").extract()[1]
    num = 0
    return car

  def init_car(self):
    car = Car()   
    init_base_item(car)
    car["title"] = car["make"] = car["model"] = car["year"] = car["drive_train"] = car["seller"] = car["price"] = car["post_date"] = car["body_style"] = car["mileage"] = car["transmission"] = car["fuel_type"] = car["cylinders"] = car["doors"] = car["ext_color"] = car["int_color"] = car["vin"] = car["warranty"] = car["color"] = ""
    car["category_id"] = "31"
    car["image_urls"] = car["image_names"] = car["images"] = []
    car["region"] = "Dubai"
    return car

  def get_transmission(self, trans_str):
    if trans_str.upper().startswith('A'):
      return "AUTO"
    return "MANUAL"
  
  def get_fuel_type(self, fuel_type_str):
    if fuel_type_str.upper().startswith('D'):
      return 'DIESEL'
    elif (fuel_type_str.upper().startswith('G') or fuel_type_str.upper().startswith('P')):
      return 'GASOLINE'
    elif fuel_type_str.upper().startswith('E'):
      return 'ELECTRIC-HYBRID'
    else:
      return 'OTH'
  
  def get_warranty(self, warranty_str):
    if str != "":
      return 1
    return 0

class ExpatriatesjobsSpider(scrapy.Spider):
  #name of the spider
  name = "expatriatesjobs"

  #list of allowed domains
  allowed_domains = ["www.expatriates.com"]
  
  #list of urls
  start_urls = [
    "http://www.expatriates.com/classifieds/uae/jobs/",
  ]
  
  #domain url
  domain = "www.expatriates.com"
  
  def parse(self, response):
    total_page = 0
    if len(response.xpath('//nav[@class="pagination"]//a//text()').extract()) > 0:
      total_page = max(map((lambda x: int(x) if x.isdigit() else 0), response.xpath('//nav[@class="pagination"]//a//text()').extract()))
    if total_page == 0:
      url = "http://www.expatriates.com/classifieds/uae/jobs"
      request = scrapy.Request(url, callback=self.parse_each_page)
      yield request
    else:
      for page in range(1,total_page):
        if page == 1:
          url = "http://www.expatriates.com/classifieds/uae/jobs/index.html"
        else: 
          url = "http://www.expatriates.com/classifieds/uae/jobs/index%s.html" % (page-1)*100
        request = scrapy.Request(url, callback=self.parse_each_page)
        yield request

  def parse_each_page(self, response):
    for item in response.xpath('//a[contains(./@href, "cls")]'):
      item_detail_url = "http://www.expatriates.com" + validate(item.xpath('./@href'))
      request = scrapy.Request(item_detail_url, callback=self.parse_job_detail)
      yield request


  def parse_job_detail(self, response):
    job = self.init_job()
    job["title"] = validate(response.xpath('//div[contains(@class,"page-title")]//text()[2]'))
    job["post_date"] = correct_date(validate(response.xpath('//li[contains(./strong/text(),"Date")]/text()')))
    job["details"] = " ".join(response.xpath('//div[@class="post-body"]//text()').extract()).strip()
    images = response.xpath('//div[@class="posting-images top-margin"]//img/@src').extract()
    job["image_urls"] =  map((lambda x: "http://www.expatriates.com%s" % x), images)
    job["contact_email"] = ""
    job["expiration_date"] = ""

    job["name"] = ""
    job["job_type"] = validate(response.xpath('//div[@class="post-body"]//text()[2]'))
    job["status"] = ""
    job["category"] = validate(response.xpath('//li[contains(./strong/text(),"Category")]/text()'))
    job["salary"] = validate(response.xpath("//div[@class='post-body']//text()[6]"))
    job["location"] = validate(response.xpath('//li[contains(./strong/text(),"Region")]/text()'))
    job["code"] = validate(response.xpath('//li[contains(./strong/text(),"Posting ID")]/text()'))
    job["contact_number"] = ""
    job["region"] = response.xpath('//li[contains(./strong/text(),"Region")]/text()').extract_first().split(" ")[1]
    if len(response.xpath("//button[contains(./@class,'posting-phone')]//text()").extract()) >=2: 
      job["contact_number"] = response.xpath("//button[contains(./@class,'posting-phone')]//text()").extract()[1]
    return job

  def init_job(self):
    job = Job()
    init_base_item(job)
    job["item_type"] = "job"
    job["title"] = job["name"] = job["post_date"] = job["job_type"] = job["status"] = job["category"] = job["salary"] = job["price"] = job["details"] = ""
    job["category_id"] = "3"
    job["e_relation"] = "LOOKING"
    job["fk_c_locale_code"] = "en_US"
    job["image_urls"] = job["image_names"] = job["images"] = []
    job["s_desired_exp"] = job["s_studies"] = job["s_minimum_requirements"] = job["s_desired_requirements"] = job["s_contract"] = job["s_company_description"] = "" 
    job["region"] = "Dubai"
    return job

class ExpatriatespropertySpider(scrapy.Spider):
  #name of the spider
  name = "expatriatesproperty"

  #list of allowed domains
  allowed_domains = ["www.expatriates.com"]
  
  #list of urls
  start_urls = [
    "http://www.expatriates.com/classifieds/uae/hs/",
  ]
  
  #domain url
  domain = "http://www.expatriates.com/"

  def parse(self, response):
    total_page = 0
    if len(response.xpath('//nav[@class="pagination"]//a//text()').extract()) > 0:
      total_page = max(map((lambda x: int(x) if x.isdigit() else 0), response.xpath('//nav[@class="pagination"]//a//text()').extract()))

    if total_page == 0:
      url = "http://www.expatriates.com/classifieds/uae/hs"
      request = scrapy.Request(url, callback=self.parse_each_page)
      yield request
    else:
      for page in range(1,total_page):
        if page == 1:
          url = "http://www.expatriates.com/classifieds/uae/hs/index.html"
        else: 
          url = "http://www.expatriates.com/classifieds/uae/hs/index%s.html" % (page-1)*100
        request = scrapy.Request(url, callback=self.parse_each_page)
        yield request

  def parse_each_page(self, response):
    for item in response.xpath('//a[contains(./@href, "cls")]'):
      item_detail_url = "http://www.expatriates.com" + validate(item.xpath('./@href'))
      request = scrapy.Request(item_detail_url, callback=self.parse_house_detail)
      yield request

  def parse_house_detail(self, response):
    house = self.init_house()
    house["category"] = validate(response.xpath('//li[contains(./strong/text(),"Category")]/text()'))
    house["item_type"] = "house"
    house["title"] = validate(response.xpath('//div[contains(@class,"page-title")]//text()[2]'))
    house["post_date"] = correct_date(validate(response.xpath('//li[contains(./strong/text(),"Date")]/text()')))
    house["price"] =  ""
    house["details"] = " ".join(response.xpath('//div[@class="post-body"]//text()').extract()).strip()
    house["contact_name"] = ""
    house["contact_email"] = ""
    images = response.xpath('//div[@class="posting-images top-margin"]//img/@src').extract()
    house["image_urls"] =  map((lambda x: "http://www.expatriates.com%s" % x), images)    
    
    house["name"] = ""
    house["yearly_rent"] = ""
    house["plot_size"] = ""
    house["sub_community"] = validate(response.xpath('//li[contains(./strong/text(),"Region")]/text()'))
    house["parking"] = ""
    house["reference_number"] = ""
    house["bedroom"] = ""
    house["full_baths"] = ""
    house["square_feet"] = ""
    house["contact_number"] = ""
    house["region"] = response.xpath('//li[contains(./strong/text(),"Region")]/text()').extract_first().split(" ")[1]
    if len(response.xpath("//button[contains(./@class,'posting-phone')]//text()").extract()) >=2: 
      house["contact_number"] = response.xpath("//button[contains(./@class,'posting-phone')]//text()").extract()[1]
    return house

  def init_house(self):
    house = House()
    init_base_item(house)
    house["name"] = house["yearly_rent"] = house["post_date"] = house["plot_size"] = house["sub_community"] = house["reference_number"] = house["bedroom"] = house["square_feet"] = house["parking"] = ""
    house["region"] = "Dubai"
    return house


class ExpatriatesclassifiedsSpider(scrapy.Spider):
  #name of the spider
  name = "expatriatesclassifieds"

  #list of allowed domains
  allowed_domains = ["www.expatriates.com"]
  
  #list of urls
  start_urls = [
    "http://www.expatriates.com/classifieds/uae/",
  ]
  
  #domain url
  domain = "http://www.expatriates.com/"

  def parse(self, response):
    for category in response.xpath('//div[@class="categories clearfix"]//li/a'):
      url = "http://www.expatriates.com" + validate(category.xpath('./@href'))
      request = scrapy.Request(url, callback=self.parse_category)
      request.meta["category_name"]=validate(category.xpath('./text()'))
      yield request

  def parse_category(self, response):
    total_page = 0
    if len(response.xpath('//nav[@class="pagination"]//a//text()').extract()) > 0:
      total_page = max(map((lambda x: int(x) if x.isdigit() else 0), response.xpath('//nav[@class="pagination"]//a//text()').extract()))

    if total_page == 0:
      url = response.url
      request = scrapy.Request(url, callback=self.parse_each_page)
      yield request
    else:
      for page in range(1,total_page):
        if page == 1:
          url = "%sindex.html" % response.url
        else:
          url = "%sindex%s.html" % (response.url, (page-1)*100)
        request = scrapy.Request(url, callback=self.parse_each_page)
        yield request

  def parse_each_page(self, response):
    for item in response.xpath('//a[contains(./@href, "cls")]'):
      item_detail_url = "http://www.expatriates.com" + validate(item.xpath('./@href'))
      request = scrapy.Request(item_detail_url, callback=self.parse_classified_detail)
      yield request

  def parse_classified_detail(self, response):
    classified = self.init_classified()
    classified["category_id"] = ""
    classified["category"] = validate(response.xpath('//li[contains(./strong/text(),"Category")]/text()'))
    classified["item_type"] = "classifieds"
    classified["title"] = validate(response.xpath('//div[contains(@class,"page-title")]//text()[2]'))
    classified["post_date"] = correct_date(validate(response.xpath('//li[contains(./strong/text(),"Date")]/text()')))
    classified["price"] = ""
    classified["details"] = " ".join(response.xpath('//div[@class="post-body"]//text()').extract()).strip()
    images = response.xpath('//div[@class="posting-images top-margin"]//img/@src').extract()
    classified["image_urls"] =  map((lambda x: "http://www.expatriates.com%s" % x), images)

    classified["contact_name"] = ""
    classified["contact_email"] = ""

    classified["location"] = validate(response.xpath('//li[contains(./strong/text(),"Region")]/text()'))
    classified["contact"] = ""
    classified["contact_number"] = ""
    classified["region"] = response.xpath('//li[contains(./strong/text(),"Region")]/text()').extract_first().split(" ")[1]
    if len(response.xpath("//button[contains(./@class,'posting-phone')]//text()").extract()) >=2: 
      classified["contact_number"] = response.xpath("//button[contains(./@class,'posting-phone')]//text()").extract()[1]
    return classified

  def init_classified(self):
    classified = Classified()
    init_base_item(classified)
    classified["title"] = classified["location"] = classified["category"] = classified["post_date"] = classified["contact"] = ""
    classified["image_urls"] = classified["images"] = []
    classified["region"] = "Dubai"
    return classified