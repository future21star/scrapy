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
  return datetime.strptime(ori_date, '%B %d, %Y %I:%M %p').strftime('%Y-%m-%d %H:%M:%S')

#Get Price from str
def extract_price(pri_str):
  return pri_str.split(" ", 1)[-1]

#Initialize BaseItem Class
def init_base_item(item):
  item["category_id"] = item["category"] = item["item_type"] = item["title"] = item["post_date"] = item["price"] = item["details"] = item["contact_name"] = item["contact_email"] = item["expiration_date"] = ""
  item["image_urls"] = item["image_names"] = item["images"] = [] 

class KhaleejcarsSpider(scrapy.Spider):
  #name of the spider
  name = "khaleejcars"

  #list of allowed domains
  allowed_domains = ["buzzon.khaleejtimes.com"]
  
  #list of urls
  start_urls = [
    "http://buzzon.khaleejtimes.com/ad-category/used-cars/",
  ]

  #domain url
  domain = "http://buzzon.khaleejtimes.com"

  def parse(self, response):
    total_num = validate(response.xpath('.//h1[@class="single dotted"]/text()'))
    total_num = unicodedata.normalize('NFKD', total_num).encode('ascii','ignore')
    total_num = int(filter(str.isdigit, total_num))
    page_num = total_num / 15
    if total_num % 15 != 0:
      page_num += 1 
    for page in range(1, page_num):
      url = "http://buzzon.khaleejtimes.com/ad-category/used-cars/page/%s/" % page
      request = scrapy.Request(url, callback=self.parse_each_page)
      request.meta["page"]=page
      yield request

  #parse main car list
  #get the make and made info for each car
  def parse_each_page(self, response):
    page = response.meta["page"]
    for car in response.xpath('//div[@class="content_left"]//div[contains(@class, "post-block-out ")]'):
      car_detail_url = validate(car.xpath('.//a/@href'))
      request = scrapy.Request(car_detail_url, callback=self.parse_car_detail)
      yield request

  #parse car detail
  #get the detail info for each car
  def parse_car_detail(self, response):
    car = self.init_car()
    car["category"] = "Cars"
    car["item_type"] = "car"
    car["title"] = validate(response.xpath('//h1[@class="single-listing"]/a/text()'))
    car["make"] = validate(response.xpath('//li[@id="cp_car_brand"]/text()'))
    car["model"] = validate(response.xpath('//li[@id="cp_model"]/text()'))
    car["price"] = extract_price(validate(response.xpath('//p[@class="post-price"]/text()')))
    car["year"] = validate(response.xpath('//li[@id="cp_year"]/text()'))
    car["body_style"] = validate(response.xpath('//li[@id="cp_body_style"]/text()'))
    car["seller"] = validate(response.xpath('//li[@id="cp_seller_type"]/text()'))
    car["doors"] = validate(response.xpath('//li[@id="cp_doors"]/text()'))
    car["color"] = validate(response.xpath('//li[@id="cp_color"]/text()'))
    car["mileage"] = validate(response.xpath('//li[@id="cp_mileage"]/text()'))
    car["warranty"] = validate(response.xpath('//li[@id="cp_warranty"]/text()'))
    car["contact_number"] = validate(response.xpath('//li[@id="cp_contact_no"]/text()'))
    car["contact_email"] = validate(response.xpath('//li[@id="cp_email_address"]/a/text()'))
    car["mileage"] = validate(response.xpath('//li[@id="cp_mileage"]/text()'))
    car["post_date"] = correct_date(validate(response.xpath('//li[@id="cp_listed"]/text()')))
    car["expiration_date"] = validate(response.xpath('//li[@id="cp_expires"]/text()'))
    car["image_urls"] = response.xpath('//div[@class="bigleft"]//img/@src').extract()
    car["details"] = " ".join(response.xpath('//div[@class="single-main"]//p/text()').extract())
    num = 0
    return car

  def init_car(self):
    car = Car()   
    init_base_item(car)
    car["title"] = car["make"] = car["model"] = car["year"] = car["drive_train"] = car["seller"] = car["price"] = car["post_date"] = car["body_style"] = car["mileage"] = car["transmission"] = car["fuel_type"] = car["cylinders"] = car["doors"] = car["ext_color"] = car["int_color"] = car["vin"] = car["warranty"] = car["color"] = ""
    car["category_id"] = "31"
    car["image_urls"] = car["image_names"] = car["images"] = []
    car["region"] = ""
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

class KhaleejjobsSpider(scrapy.Spider):
  #name of the spider
  name = "khaleejjobs"

  #list of allowed domains
  allowed_domains = ["buzzon.khaleejtimes.com"]
  
  #list of urls
  start_urls = [
    "http://buzzon.khaleejtimes.com/ad-category/jobs-vacancies/",
  ]
  
  #domain url
  domain = "buzzon.khaleejtimes.com"
    
  def parse(self, response):
    total_num = validate(response.xpath('.//h1[@class="single dotted"]/text()'))
    total_num = unicodedata.normalize('NFKD', total_num).encode('ascii','ignore')
    total_num = int(filter(str.isdigit, total_num))
    page_num = total_num / 15
    if total_num % 15 != 0:
      page_num += 1 
    for page in range(1, page_num):
      url = "http://buzzon.khaleejtimes.com/ad-category/jobs-vacancies/page/%s/" % page
      request = scrapy.Request(url, callback=self.parse_each_page)
      request.meta["page"]=page
      yield request

  #parse main car list
  #get the make and made info for each car
  def parse_each_page(self, response):
    page = response.meta["page"]
    for job in response.xpath('//div[@class="content_left"]//div[contains(@class, "post-block-out ")]'):
      job_detail_url = validate(job.xpath('.//a/@href'))
      request = scrapy.Request(job_detail_url, callback=self.parse_job_detail)
      yield request

  def parse_job_detail(self, response):
    job = self.init_job()
    job["category"] = map((lambda x:x.strip()),response.xpath('//div[@itemprop="breadcrumb"]//a//text()').extract())[-1]
    job["title"] = validate(response.xpath('//h1[@class="single-listing"]/a/text()'))
    job["post_date"] = correct_date(validate(response.xpath('//li[@id="cp_listed"]/text()')))
    job["details"] = " ".join(response.xpath('//div[@class="single-main"]//p/text()').extract())
    job["image_urls"] = response.xpath('//div[@class="bigleft"]//img/@src').extract()
    job["contact_email"] = validate(response.xpath('//li[@id="cp_email_address"]/a/text()'))
    job["expiration_date"] = validate(response.xpath('//li[@id="cp_expires"]/text()'))

    job["name"] = validate(response.xpath('//li[@id="cp_industry"]/text()'))
    job["job_type"] = validate(response.xpath('//li[@id="cp_career"]/text()'))
    job["status"] = validate(response.xpath('//li[@id="cp_job_type"]/text()'))
    job["salary"] = validate(response.xpath('//li[@id="cp_salary"]/text()'))
    job["location"] = validate(response.xpath('//li[@id="cp_job_location"]/text()'))
    job["code"] = ""
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
    job["region"] = ""
    return job

class KhaleejpropertySpider(scrapy.Spider):
  #name of the spider
  name = "khaleejproperty"

  #list of allowed domains
  allowed_domains = ["buzzon.khaleejtimes.com"]
  
  #list of urls
  start_urls = [
    "http://buzzon.khaleejtimes.com/ad-category/real-estate/",
  ]
  
  #domain url
  domain = "http://buzzon.khaleejtimes.com/"

  def parse(self, response):
    total_num = validate(response.xpath('.//h1[@class="single dotted"]/text()'))
    total_num = unicodedata.normalize('NFKD', total_num).encode('ascii','ignore')
    total_num = int(filter(str.isdigit, total_num))
    page_num = total_num / 15
    if total_num % 15 != 0:
      page_num += 1 
    for page in range(1, page_num):
      url = "http://buzzon.khaleejtimes.com/ad-category/real-estate/page/%s/" % page
      request = scrapy.Request(url, callback=self.parse_each_page)
      request.meta["page"]=page
      yield request

  #parse main car list
  #get the make and made info for each car
  def parse_each_page(self, response):
    page = response.meta["page"]
    for house in response.xpath('//div[@class="content_left"]//div[contains(@class, "post-block-out ")]'):
      house_detail_url = validate(house.xpath('.//a/@href'))
      request = scrapy.Request(house_detail_url, callback=self.parse_house_detail)
      yield request

  def parse_house_detail(self, response):
    house = self.init_house()
    category = map((lambda x:x.strip()),response.xpath('//div[@itemprop="breadcrumb"]//a//text()').extract())[-1]
    house["category"] = category
    house["item_type"] = "house"
    house["title"] = validate(response.xpath('//h1[@class="single-listing"]/a/text()'))
    house["post_date"] = correct_date(validate(response.xpath('//li[@id="cp_listed"]/text()')))
    house["price"] =  extract_price(validate(response.xpath('//p[@class="post-price"]/text()')))
    house["details"] = " ".join(response.xpath('//div[@class="single-main"]//p/text()').extract())
    house["contact_name"] = ""
    house["contact_email"] = validate(response.xpath('//li[@id="cp_email_address"]/text()'))
    house["image_urls"] = response.xpath('//div[@class="bigleft"]//img/@src').extract()
    
    house["name"] = ""
    house["yearly_rent"] = ""
    house["plot_size"] = ""
    house["sub_community"] = validate(response.xpath('//li[@id="cp_city"]/text()'))
    house["parking"] = validate(response.xpath('//li[@id="cp_parking"]/text()')).split(" ")[0]
    house["reference_number"] = ""
    house["bedroom"] = validate(response.xpath('//li[@id="cp_bedrooms"]/text()')).split(" ")[0]
    house["full_baths"] = validate(response.xpath('//li[@id="cp_bathroom"]/text()'))
    house["square_feet"] = validate(response.xpath('//li[@id="cp_area_in_sq_ft"]/text()'))
    house["region"] = ""
    return house

  def init_house(self):
    house = House()
    init_base_item(house)
    house["name"] = house["yearly_rent"] = house["post_date"] = house["plot_size"] = house["sub_community"] = house["reference_number"] = house["bedroom"] = house["square_feet"] = house["parking"] = ""
    return house


class KhaleejclassifiedsSpider(scrapy.Spider):
  #name of the spider
  name = "khaleejclassifieds"

  #list of allowed domains
  allowed_domains = ["buzzon.khaleejtimes.com"]
  
  #list of urls
  start_urls = [
    "http://buzzon.khaleejtimes.com/categories/",
  ]
  
  #domain url
  domain = "http://buzzon.khaleejtimes.com/"

  def parse(self, response):
    for each_category_url in response.xpath('//div[@id="directory"]//a/@href').extract():
      request = scrapy.Request(each_category_url, callback=self.parse_each_category)
      yield request

  def parse_each_category(self, response):
    total_num = validate(response.xpath('.//h1[@class="single dotted"]/text()'))
    total_num = unicodedata.normalize('NFKD', total_num).encode('ascii','ignore')
    total_num = int(filter(str.isdigit, total_num))
    page_num = total_num / 15
    if total_num % 15 != 0:
      page_num += 1 
    for page in range(1, page_num):
      url = "%spage/%s/" % (response.url, page)
      request = scrapy.Request(url, callback=self.parse_each_page)
      request.meta["page"]=page
      yield request

  #parse main car list
  #get the make and made info for each car
  def parse_each_page(self, response):
    page = response.meta["page"]
    for classified in response.xpath('//div[@class="content_left"]//div[contains(@class, "post-block-out ")]'):
      classified_detail_url = validate(classified.xpath('.//a/@href'))
      request = scrapy.Request(classified_detail_url, callback=self.parse_classified_detail)
      yield request

  def parse_classified_detail(self, response):
    classified = self.init_classified()
    classified["category_id"] = ""
    classified["category"] = map((lambda x:x.strip()),response.xpath('//div[@itemprop="breadcrumb"]//a//text()').extract())[1]
    classified["item_type"] = "classifieds"
    classified["title"] = validate(response.xpath('//h1[@class="single-listing"]/a/text()'))
    classified["post_date"] = correct_date(validate(response.xpath('//li[@id="cp_listed"]/text()')))
    classified["price"] = extract_price(validate(response.xpath('//p[@class="post-price"]/text()')))
    classified["details"] = " ".join(response.xpath('//div[@class="single-main"]//p/text()').extract())
    classified["image_urls"] = response.xpath('//div[@class="bigleft"]//img/@src').extract()
    classified["contact_name"] = ""
    classified["contact_email"] = validate(response.xpath('//li[@id="cp_email_address"]/text()'))

    classified["location"] = validate(response.xpath('//li[@id="cp_job_location"]/text()'))
    classified["contact"] = ""
    return classified

  def init_classified(self):
    classified = Classified()
    init_base_item(classified)
    classified["title"] = classified["location"] = classified["category"] = classified["post_date"] = classified["contact"] = ""
    classified["image_urls"] = classified["images"] = []
    classified["region"] = ""
    return classified