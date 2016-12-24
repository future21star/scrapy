# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
import pdb

class BaseItem(scrapy.Item):
  category_id = scrapy.Field()
  category = scrapy.Field()
  item_type = scrapy.Field()
  title = scrapy.Field()
  post_date = scrapy.Field()
  price = scrapy.Field()
  details = scrapy.Field()
  contact_name = scrapy.Field()
  contact_email = scrapy.Field()
  contact_number = scrapy.Field()
  image_urls = scrapy.Field()
  image_names = scrapy.Field()
  images = scrapy.Field()
  expiration_date = scrapy.Field()
  region = scrapy.Field()

class Car(BaseItem):
  make = scrapy.Field()
  model = scrapy.Field()
  year = scrapy.Field()
  drive_train = scrapy.Field()
  seller = scrapy.Field()
  body_style = scrapy.Field()
  mileage = scrapy.Field()
  transmission = scrapy.Field()
  fuel_type = scrapy.Field()
  cylinders = scrapy.Field()
  doors = scrapy.Field()
  ext_color = scrapy.Field()
  int_color = scrapy.Field()
  vin = scrapy.Field()
  warranty = scrapy.Field()
  color = scrapy.Field()

class Job(BaseItem):
  name = scrapy.Field()
  job_type = scrapy.Field()
  status = scrapy.Field()
  category = scrapy.Field()
  salary = scrapy.Field()
  location = scrapy.Field()
  code = scrapy.Field()
  category_id = scrapy.Field()
  e_relation = scrapy.Field()
  #description table
  s_desired_exp = scrapy.Field()
  s_studies = scrapy.Field()
  s_minimum_requirements = scrapy.Field()
  s_desired_requirements = scrapy.Field()
  s_contract = scrapy.Field()
  s_company_description = scrapy.Field()
  fk_c_locale_code = scrapy.Field()

class Classified(BaseItem):
  location = scrapy.Field()
  contact = scrapy.Field()

class House(BaseItem):
  name = scrapy.Field()
  yearly_rent = scrapy.Field()
  plot_size = scrapy.Field()
  sub_community = scrapy.Field()
  reference_number = scrapy.Field()
  bedroom = scrapy.Field()
  full_baths = scrapy.Field()
  square_feet = scrapy.Field()
  parking = scrapy.Field()
