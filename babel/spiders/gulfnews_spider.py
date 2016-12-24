import scrapy
import pdb
from babel.items import *
from datetime import datetime 
import re

# validate the value of html node
#		return string value, if data is validated
#		return "", otherwise
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
	return datetime.strptime(ori_date, '%d-%m-%Y').strftime('%Y-%m-%d %H:%M:%S')

#Get Price from str
def extract_price(pri_str):
	return pri_str.split(" ", 1)[-1]

#Initialize BaseItem Class
def init_base_item(item):
	item["category_id"] = item["category"] = item["item_type"] = item["title"] = item["post_date"] = item["price"] = item["details"] = item["contact_name"] = item["contact_email"] = ""
	item["image_urls"] = item["image_names"] = item["images"] = [] 

class GncarsSpider(scrapy.Spider):
	#name of the spider
	name = "gncars"

	#list of allowed domains
	allowed_domains = ["www.gncars.com"]
	
	#domain url
	domain = "http://www.gncars.com"
	def start_requests(self):
		urls = [
			'http://www.gncars.com/motors/results/car?radius=25'
		]
		requests = []
		for page in range(1,190):
			url = "http://www.gncars.com/motors/results/car?radius=25&page=%s" % page
			request = scrapy.Request(url, callback=self.parse)
			request.meta["page"]=page
			requests.append(request)
			
		return requests

	#parse main car list
	#get the make and made info for each car
	def parse(self, response):
		page = response.meta["page"]
		for car in response.xpath('//div[@class="aiResultsMainDiv"]'):
			car_detail_url = "http://www.gncars.com" + validate(car.xpath('.//a[@class="aiPreventOnclick"]//@href[1]'))
			
			ai_results_description = car.xpath('.//div[@class="aiResultsDescription"]')
			
			car_title_info = validate(car.xpath('.//a[@class="aiPreventOnclick"]/text()'))
			car_year_info = validate(ai_results_description.xpath('./div[contains(./span/text()[1], "Year")]/text()[2]'))
			car_make_info = validate(ai_results_description.xpath('./div[contains(./span/text()[1], "Make")]/text()[2]'))
			car_model_info = validate(ai_results_description.xpath('./div[contains(./span/text(), "Model")]/text()[2]'))
			car_drive_train = validate(ai_results_description.xpath('./div[contains(./span/text(), "Drive Train")]/text()[2]'))
			car_seller = validate(response.xpath('.//div[@class="contactLinks"]/strong/a/text()')) + ":" + validate(response.xpath('.//div[@class="contactLinks"]/strong[2]/text()'))			
			request = scrapy.Request(car_detail_url, callback=self.parse_car_detail)
			request.meta["title"] = car_title_info
			request.meta["year"] = car_year_info
			request.meta["make"] = car_make_info
			request.meta["model"] = car_model_info
			request.meta["drive_train"] = car_drive_train
			request.meta["seller"] = car_seller
			yield request

	#parse car detail
	#get the detail info for each car
	def parse_car_detail(self, response):
		car = self.init_car()
		car["category"] = "Cars"
		car["item_type"] = "car"
		car["title"] = response.meta["title"]
		car["make"] = response.meta["make"]
		car["model"] = response.meta["model"]
		car["year"] = response.meta["year"]
		car["drive_train"] = response.meta["drive_train"]
		car["seller"] = response.meta["seller"]
		car["price"] = extract_price(validate(response.xpath('//span[@class="aiDetailCurrentPrice"]/text()')))
		car["post_date"] = correct_date(validate(response.xpath('//tr[contains(./td/text(),"Post Date")]/td[@class="detailTabContentRt"]/text()')))
		car["body_style"] = validate(response.xpath('//tr[contains(./td/text(),"Body Style")]/td[@class="detailTabContentRt"]/text()'))
		car["mileage"] = validate(response.xpath('//tr[contains(./td/text(),"Mileage")]/td[@class="detailTabContentRt"]/text()'))
		car["transmission"] = self.get_transmission(validate(response.xpath('//tr[contains(./td/text(),"Transmission")]/td[@class="detailTabContentRt"]/text()')))
		car["fuel_type"] = self.get_fuel_type(validate(response.xpath('//tr[contains(./td/text(),"Fuel Type")]/td[@class="detailTabContentRt"]/text()')))
		car["cylinders"] = validate(response.xpath('//tr[contains(./td/text(),"Cylinders")]/td[@class="detailTabContentRt"]/text()'))
		car["doors"] = validate(response.xpath('//tr[contains(./td/text(),"Doors")]/td[@class="detailTabContentRt"]/text()'))
		car["ext_color"] = validate(response.xpath('//tr[contains(./td/text(),"Ext. Color")]/td[@class="detailTabContentRt"]/text()'))
		car["int_color"] = validate(response.xpath('//tr[contains(./td/text(),"Int. Color")]/td[@class="detailTabContentRt"]/text()'))
		car["vin"] = validate(response.xpath('//tr[contains(./td/text(),"VIN")]/td[@class="detailTabContentRt"]/text()'))
		car["warranty"] = self.get_warranty(validate(response.xpath('//tr[contains(./td/text(),"Warranty")]/td[@class="detailTabContentRt"]/text()')))
		car["image_urls"] = response.xpath('//div[@class="aiDetailsMedImage aiImageLetterbox aiClearfix"]//img/@src').extract()
		car["contact_name"] =response.xpath('//div[contains(@class, "aiDetailContactVehicleInfo")]//div[2]//div[@class="aiFormRowContent"]//text()').extract()[3].strip()
		car["contact_email"] = ""
		car["details"] = response.xpath('//div[@class="aiDetailsDescription"]//text()').extract()[-1].strip()
		num = 0
		return car

	def init_car(self):
		car = Car() 	
		init_base_item(car)
		car["title"] = car["make"] = car["model"] = car["year"] = car["drive_train"] = car["seller"] = car["price"] = car["post_date"] = car["body_style"] = car["mileage"] = car["transmission"] = car["fuel_type"] = car["cylinders"] = car["doors"] = car["ext_color"] = car["int_color"] = car["vin"] = car["warranty"] = ""
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

class GncareersSpider(scrapy.Spider):
	#name of the spider
	name = "gncareers"

	#list of allowed domains
	allowed_domains = ["www.gncareers.com"]
	
	#list of urls
	start_urls = [
		"http://www.gncareers.com/jobs/search/results",
	]
	
	#domain url
	domain = "http://www.gncareers.com/jobs/search/results"
		
	def parse(self, response):
		total_num = int(validate(response.xpath('.//span[@id="retCountNumber"]/text()')))
		page_num = total_num / 15
		if total_num % 15 != 0:
			page_num += 1 
		for page in range(1, page_num):
			url = "http://www.gncareers.com/jobs/search/results?page=%s" % page
			request = scrapy.Request(url, callback=self.parse_each_page)
			request.meta["page"]=page
			yield request

	def parse_each_page(self, response):
		for job in response.xpath('//div[contains(@class, "aiDevFeaturedSection")]'):
			job_detail_url = "http://www.gncareers.com" + validate(job.xpath('.//div[contains(@class, "aiResultTitle")]/h3/a/@href'))
			request = scrapy.Request(job_detail_url, callback=self.parse_job_detail)
			yield request

	def parse_job_detail(self, response):
		detail_info = response.xpath(".//div[contains(@class, aiDetailTopInfo)]")
		job = self.init_job()
		job["category"] = "Jobs"
		job["title"] = validate(detail_info.xpath('.//span[contains(@itemprop, "title")]/text()'))
		job["post_date"] = correct_date(validate(detail_info.xpath('.//span[contains(@itemprop, "datePosted")]/text()')))
		job["details"] = " ".join(response.xpath('//div[@id="detailTab"]//span[@itemprop="description"]//text()').extract())
		job["image_urls"] = response.xpath('//div[@class="aiDetailsMedImage aiImageLetterbox aiClearfix"]//img/@src').extract()
		match = re.search(r'[\w\.-]+@[\w\.-]+', " ".join(response.xpath('//div[@id="detailTab"]//text()').extract()))
		if match is not None:
			job["contact_email"] = match.group(0)
		
		job["name"] = validate(detail_info.xpath('.//span[contains(@itemprop, "name")]/text()'))
		job["job_type"] = validate(detail_info.xpath('.//span[contains(@itemprop, "employmentType")][1]/text()'))
		job["status"] = validate(detail_info.xpath('.//span[contains(@itemprop, "employmentType")][2]/text()'))
		job["category"] = validate(detail_info.xpath('.//span[contains(@itemprop, "occupationalCategory")]/text()'))
		job["salary"] = validate(detail_info.xpath('.//span[contains(@itemprop, "baseSalary")]/text()'))
		job["location"] = validate(detail_info.xpath('.//span[contains(@itemprop, "addressLocality")]/text()'))
		job["code"] = validate(detail_info.xpath('.//div[@class="aiDetailJobInfo aiDetailJobInfoJobCode"]/text()'))
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

class GnclassifiedsSpider(scrapy.Spider):
	#name of the spider
	name = "gnclassifieds"

	#list of allowed domains
	allowed_domains = ["www.gnclassifieds.com"]
	
	#list of urls
	start_urls = [
		"http://www.gnclassifieds.com/g/browse/all/ads?kwsPrimary=&catsubcat=&button=Find&type=classifiedsearch&widget=1&widgetref=",
	]
	
	#domain url
	domain = "http://www.gnclassifieds.com/"

	def parse(self, response):
		total_num = [int(s) for s in validate(response.xpath('.//span[@id="retCount"]/text()')).split() if s.isdigit()][0]
		page_num = total_num / 15
		if total_num % 15 != 0:
			page_num += 1 
		for page in range(1, page_num):
			url = "http://www.gnclassifieds.com/g/browse/all/ads?button=Find&page=%s" % page
			request = scrapy.Request(url, callback=self.parse_each_page)
			request.meta["page"]=page
			yield request

	def parse_each_page(self, response):
		for classified in response.xpath('//div[contains(@class, "aiResultTitle")]'):
			classified_detail_url = "http://www.gnclassifieds.com" + validate(classified.xpath('.//h3/a/@href'))
			request = scrapy.Request(classified_detail_url, callback=self.parse_classified_detail)
			yield request

	def parse_classified_detail(self, response):
		classified = self.init_classified()
		classified["category_id"] = ""
		classified["item_type"] = "classifieds"
		classified["title"] = validate(response.xpath('//div[contains(@class,"aiDetailPageTitle")]/h1/text()'))
		classified["post_date"] = correct_date(validate(response.xpath('.//tr[contains(./td/text(),"Posted")]/td[@class="detailTabContentRt"]/text()')))
		classified["price"] = extract_price(validate(response.xpath('//span[@class="aiDetailCurrentPrice"]//text()')))
		classified["details"] = "".join(response.xpath('//div[@class="aiDetailsDescription"]//text()').extract()).replace("\n", "").strip()[7:].strip()
		classified["image_urls"] = response.xpath('//div[@class="aiDetailsMedImage aiImageLetterbox aiClearfix"]//img/@src').extract()
		if len(classified["image_urls"]) == 0:
			classified["image_urls"].append("http://c0647ec93d74ba0b2ce5-9d25a0da500d22404b4ba3af6efd73c1.r37.cf3.rackcdn.com/css/../img/no-image.jpg")
		classified["contact_name"] = ""
		classified["contact_email"] = ""
		classified["category"] = validate(response.xpath('.//tr[contains(./td/text(),"Category")]/td[@class="detailTabContentRt"]/text()'))

		classified["location"] = validate(response.xpath('.//tr[contains(./td/text(),"Location")]/td[@class="detailTabContentRt"]/text()'))
		classified["contact_number"] = validate(response.xpath('.//tr[contains(./td/text(),"Contact")]/td[@class="detailTabContentRt"]/text()'))
		return classified

	def init_classified(self):
		classified = Classified()
		init_base_item(classified)
		classified["title"] = classified["location"] = classified["category"] = classified["post_date"] = classified["contact"] = ""
		classified["image_urls"] = classified["images"] = []
		classified["region"] = ""
		return classified

class GnpropertySpider(scrapy.Spider):
	#name of the spider
	name = "gnproperty"

	#list of allowed domains
	allowed_domains = ["www.gnproperty.com"]
	
	#list of urls
	start_urls = [
		"http://www.gnproperty.com/homes/results/united-arab-emirates/for-sale?radius=0&locationType=text",
		"http://www.gnproperty.com/homes/results/united-arab-emirates/for-rent?radius=0&locationType=text",
	]
	
	#domain url
	domain = "http://www.gnproperty.com/"

	def parse(self, response):
		total_num = int(validate(response.xpath('.//span[@id="retCountNumber"]/text()')))
		page_num = total_num / 15
		if total_num % 15 != 0:
			page_num += 1 
		for page in range(1, page_num):
			url = "http://www.gnproperty.com/homes/results/United+Arab+Emirates/rent?radius=0&locationType=text&searchLocation=United+Arab+Emirates&NormalizedCountry=AE&page=%s" % page
			if "for-sale?" in response.url:
				url = "http://www.gnproperty.com/homes/results/United+Arab+Emirates/sell?radius=0&locationType=text&searchLocation=United+Arab+Emirates&NormalizedCountry=AE&page=%s" % page
			request = scrapy.Request(url, callback=self.parse_each_page)
			request.meta["page"]=page
			yield request

	def parse_each_page(self, response):
		for house in response.xpath('//div[contains(@class, "aiResultTitle")]'):
			house_detail_url = "http://www.gnproperty.com" + validate(house.xpath('.//a/@href'))
			request = scrapy.Request(house_detail_url, callback=self.parse_house_detail)
			request.meta["category"] = validate(house.xpath('//span[@class="listingType"]/text()'))
			if "rent" in house_detail_url: 
				request.meta["category"] += " for Rent"
			else:
				request.meta["category"] += " for Sale" 
			yield request

	def parse_house_detail(self, response):
		house = self.init_house()
		house["category"] = response.meta["category"]
		house["item_type"] = "house"
		house["title"] = validate(response.xpath('//span[contains(@itemprop, "name")]/text()'))
		house["post_date"] = correct_date(validate(response.xpath('.//tr[contains(./td/text(),"Post Date")]/td[@class="detailTabContentRt"]/text()')))
		house["price"] =  extract_price(validate(response.xpath('.//tr[contains(./td/text(),"Yearly Rent")]/td[@class="detailTabContentPriceRt"]/text()')))
		if house["price"]=="":
			house["price"] =  extract_price(validate(response.xpath('.//tr[contains(./td/text(),"Current List Price")]/td[@class="detailTabContentPriceRt"]/text()')))
		house["details"] = validate(response.xpath('.//div[@class="aiDetailsDescription aiClearfix"]//span[@itemprop="description"]//text()'))
		house["contact_name"] = ""
		house["contact_email"] = ""
		house["image_urls"] = response.xpath('//div[@class="aiDetailsMedImage aiImageLetterbox aiClearfix"]//img/@src').extract()
		
		house["name"] = validate(response.xpath('//span[contains(@itemprop, "name")]/text()'))
		house["yearly_rent"] = validate(response.xpath('.//tr[contains(./td/text(),"Yearly Rent")]/td[@class="detailTabContentPriceRt"]/text()'))
		house["plot_size"] = validate(response.xpath('.//tr[contains(./td/text(),"Plot size (Sq. Ft.)")]/td[@class="detailTabContentRt"]/text()'))
		house["sub_community"] = validate(response.xpath('.//tr[contains(./td/text(),"Sub-community or Transmissionwer")]/td[@class="detailTabContentRt"]/text()'))
		house["reference_number"] = validate(response.xpath('.//tr[contains(./td/text(),"Reference Number")]/td[@class="detailTabContentRt"]/text()'))
		house["bedroom"] = validate(response.xpath('.//tr[contains(./td/text(),"Bedroom")]/td[@class="detailTabContentRt"]/text()'))
		house["full_baths"] = validate(response.xpath('.//tr[contains(./td/text(),"Full Baths")]/td[@class="detailTabContentRt"]/text()'))
		house["square_feet"] = validate(response.xpath('.//tr[contains(./td/text(),"Square Feet")]/td[@class="detailTabContentRt"]/text()'))
		house["region"] = ""
		return house

	def init_house(self):
		house = House()
		init_base_item(house)
		house["name"] = house["yearly_rent"] = house["post_date"] = house["plot_size"] = house["sub_community"] = house["reference_number"] = house["bedroom"] = house["square_feet"] = ""
		return house
