# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import sys
import MySQLdb
import hashlib
from scrapy.contrib.pipeline.images import ImagesPipeline
from PIL import Image
from scrapy.exceptions import DropItem
from scrapy.http import Request
import pdb
import cStringIO
from cStringIO import StringIO
import re
import ftplib
import os
import urllib
from categoryids import category_ids
from datetime import datetime
from datetime import timedelta
import datetime

class GulfNewsPipeline(ImagesPipeline):

  image_num = 0
  session = None
  conn = None
  cursor = None

  def process_item(self, item, spider):
    # try:
    if self.session is None:
      self.session = ftplib.FTP('ftp.badel.me','tonys','VaX8zc')
    #updating database
    self._process_items(item)
    # except:
    #   print "No Image Error"
    return item

  #Create item and relevant info
  def create_item(self, item):
    try:
      sql_con = self.cursor
      #Create item
      item["category_id"] = self.get_category_id(item["category"])
      expiration_date = datetime.datetime.strptime(item["post_date"], "%Y-%m-%d %H:%M:%S") + timedelta(days=30)
      if item["item_type"] == "job":
        item["price"] == "NA"
      if item["price"] == '' or item["price"] == "0":
        sql_con.execute('INSERT INTO oc_t_item(fk_i_user_id, fk_i_category_id, dt_pub_date, dt_mod_date, f_price, i_price, fk_c_currency_code, s_contact_name, s_contact_email, s_ip, b_premium, b_enabled, b_active, b_spam, s_secret, b_show_email, dt_expiration) VALUES ("2","%s","%s",null,null,null,null,"%s","%s","0.0.0.0","0","1","1","0","93EufZhx","0","%s")' % (item["category_id"], item["post_date"], item["contact_name"], item["contact_email"], expiration_date))
      else:
        item["price"] = int(str(item['price']).replace(",","")) * 1000000
        sql_con.execute('INSERT INTO oc_t_item(fk_i_user_id, fk_i_category_id, dt_pub_date, dt_mod_date, f_price, i_price, fk_c_currency_code, s_contact_name, s_contact_email, s_ip, b_premium, b_enabled, b_active, b_spam, s_secret, b_show_email, dt_expiration) VALUES ("2","%s","%s",null,"%s","%s","%s","%s","%s","0.0.0.0","0","1","1","0","93EufZhx","0","%s")' % (item["category_id"], item["post_date"], item["price"], item["price"], "AED", item["contact_name"], item["contact_email"], expiration_date))        
      self.conn.commit() 
      sql_con.execute('SELECT LAST_INSERT_ID()')
      fk_i_item_id = sql_con.fetchone()[0]
      #Create item description
      sql_con.execute('INSERT INTO oc_t_item_description(fk_i_item_id, fk_c_locale_code, s_title, s_description) VALUES ("%s","en_US","%s","%s")' % (fk_i_item_id, item["title"], item["details"].encode('ascii', 'ignore')))
      self.conn.commit()
      
      self.create_location(item, fk_i_item_id)
      #Create item resources  
      self.process_images(item, fk_i_item_id)
    except:
      print ":"
    return fk_i_item_id

  def get_slug_from_name_for_location(self, name):
    return "_".join(name.lower().split(" "))
  #Create Item Location   
  def create_location(self, item, pk_i_id):
    try:
      sql_con = self.cursor
      sql_con.execute('SELECT s_name, s_slug, fk_c_country_code, pk_i_id FROM oc_t_region WHERE s_name="%s"' % (item["region"]))
      region_item = sql_con.fetchone()
      if (region_item is not None):
        s_country = "Arab Emirates"
        if region_item[2] == "SA":
          s_country = "Saudi Arabia"
        sql_con.execute('INSERT INTO oc_t_item_location(fk_i_item_id, fk_c_country_code, s_country, s_address, s_zip, fk_i_region_id, s_region, fk_i_city_id, s_city, fk_i_city_area_id, s_city_area, d_coord_lat, d_coord_long) VALUES ("%s","%s","%s",NULL,NULL,"%s","%s",NULL,NULL,NULL,"%s",NULL,NULL)' % (pk_i_id, region_item[2], s_country, region_item[3], item["region"], item["contact_number"]))
        self.conn.commit()
    except:
      print ":"
    return;

  def update_item(self, item, pk_i_id):
    try:
      sql_con = self.cursor
      item["category_id"] = self.get_category_id(item["category"])
      sql_con.execute('UPDATE oc_t_item SET dt_pub_date="%s",dt_mod_date=null,f_price="%s",i_price="%s",s_contact_name="%s",s_contact_email="%s",s_ip="0.0.0.0",b_premium="0",b_enabled="1",b_active="1",b_spam="0",s_secret="93EufZhx",b_show_email="0",dt_expiration="9999-12-31 23:59:59" WHERE %s' % (item["post_date"], item["price"], item["price"], item["contact_name"], item["contact_email"], pk_i_id))
      self.conn.commit()

      sql_con.execute('UPDATE oc_t_item_description SET s_title="%s", s_description="%s" WHERE fk_i_item_id=%s' % (item["title"], item["details"], pk_i_id))
      self.conn.commit()
    except:
      print ":"
    return pk_i_id
      
  def _process_items(self, item):
    if self.conn is None:
      self.conn = MySQLdb.connect(host="mysql1204.ixwebhosting.com",
                                 user="ahennaw_badelme",         
                                 passwd="FprU5skk", 
                                 db="ahennaw_badelme")
      self.cursor = self.conn.cursor()
    if item["item_type"] == "car":
      self.process_car(item)
    elif item["item_type"] == "job":
      self.process_job(item)
    elif item["item_type"] == "classifieds":
      self.process_classifieds(item)
    elif item["item_type"] == "house":
      self.process_house(item)
    return item
  
  def process_images(self, item, fk_i_item_id):
    for image_name in item["image_urls"]:
      folder_name = self.get_new_folder_num(self.session, "/oc-content/uploads")
      self.session.cwd("/oc-content/uploads")
      self.chdir(self.session, '%s' % folder_name)

      file = cStringIO.StringIO(urllib.urlopen(image_name).read())

      img_original = Image.open(file)
      img = img_original.resize((640, 480))
      img_thumbnail = img_original.resize((240, 200))
      img_preview = img_original.resize((480, 340))
      
      content_type = 'image/jpeg'
      _content_type = content_type.split('/')[-1]
      extension = 'jpg'
      if 'PNG' in image_name.upper():
        content_type = 'image/png'
        extension = 'png'
      
      sql_con = self.cursor
      from random import choice
      from string import digits
      s_name = ''.join(choice(digits) for i in range(8))
      s_path = "/oc-content/uploads/%s" % folder_name
      sql_con.execute('INSERT INTO oc_t_item_resource(fk_i_item_id, s_name, s_extension, s_content_type, s_path) VALUES ("%s","%s","%s","%s","%s/")' % (fk_i_item_id, s_name, extension, content_type, s_path))
      self.conn.commit()
      sql_con.execute('SELECT LAST_INSERT_ID()')
      file_name = sql_con.fetchone()[0]
      
      import StringIO
      f = StringIO.StringIO()
      img.save(f, _content_type)
      f.seek(0)   
      self.session.storbinary('STOR %s.%s' % (file_name, extension), f)
      
      f = StringIO.StringIO()
      img_original.save(f, _content_type)
      f.seek(0)
      self.session.storbinary('STOR %s_original.%s' % (file_name, extension), f)
      
      f = StringIO.StringIO()
      img_thumbnail.save(f, _content_type)
      f.seek(0)
      self.session.storbinary('STOR %s_thumbnail.%s' % (file_name, extension), f)
      
      f = StringIO.StringIO()
      img_preview.save(f, _content_type)
      f.seek(0)
      self.session.storbinary('STOR %s_preview.%s' % (file_name, extension), f)

  def process_car(self, item):
    try:
      sql_con = self.cursor
      if item["make"] is not "" and item["model"] is not "" and item["body_style"] is not "":
        fk_i_make_id = self.get_corresponding_id_for_car(sql_con, item, "oc_t_item_car_make_attr", "make")
        fk_i_model_id = self.get_corresponding_id_for_car(sql_con, item, "oc_t_item_car_model_attr", "model", {"fk_i_make_id":fk_i_make_id})
        fk_vehicle_type_id = self.get_corresponding_id_for_car(sql_con, item, "oc_t_item_car_vehicle_type_attr", "body_style") 
        sql_con.execute('SELECT fk_i_item_id FROM oc_t_item_car_attr WHERE fk_i_make_id="%s" AND fk_i_model_id="%s" AND fk_vehicle_type_id="%s"' % (fk_i_make_id, fk_i_model_id, fk_vehicle_type_id))
        car_id = sql_con.fetchone()
        if car_id is None:
          fk_i_item_id = self.create_item(item)
          #Create car attr
          sql_con.execute('INSERT INTO oc_t_item_car_attr(fk_i_item_id, i_year, i_doors, i_seats, i_mileage, i_engine_size, fk_i_make_id, fk_i_model_id, i_num_airbags, e_transmission, e_fuel, e_seller, b_warranty, b_new, i_power, e_power_unit, i_gears, fk_vehicle_type_id) VALUES ("%s","%s","%s",null,"%s",null,"%s","%s",null,"%s","%s","%s","%s",null,null,null,null,"%s")' % (fk_i_item_id, item["year"], item["doors"], item["mileage"], fk_i_make_id, fk_i_model_id, item["transmission"], item["fuel_type"], item["seller"], item["warranty"], fk_vehicle_type_id))
          self.conn.commit()
        else:
          car_id = car_id[0]
          #fk_i_item_id = self.update_item(item, car_id)
          sql_con.execute('UPDATE oc_t_item_car_attr SET i_year="%s",i_doors="%s",i_seats=null,i_mileage="%s",i_engine_size=null,fk_i_make_id="%s",fk_i_model_id="%s",i_num_airbags=null,e_transmission="%s",e_fuel="%s",e_seller="%s",b_warranty="%s",b_new=null,i_power=null,e_power_unit=null,i_gears=null,fk_vehicle_type_id="%s" WHERE fk_i_item_id="%s"' % (item["year"], item["doors"], item["mileage"], fk_i_make_id, fk_i_model_id, item["transmission"], item["fuel_type"], item["seller"], item["warranty"], fk_vehicle_type_id, car_id))
          self.conn.commit()
    except:
      print ":"
    return item

  def process_job(self, item):
    try:
      sql_con = self.cursor
      sql_con.execute('SELECT fk_i_item_id FROM oc_t_item_description WHERE s_title="%s"' % item["title"])
      pk_i_id = sql_con.fetchone()
      if pk_i_id is None:
        fk_i_item_id = self.create_item(item)
        #create job attr

        sql_con.execute('INSERT INTO oc_t_item_job_attr(fk_i_item_id, e_relation, s_company_name, e_position_type, s_salary_text) VALUES ("%s", "%s", "%s", "%s", "%s")' % (fk_i_item_id, item["e_relation"], item["name"], item["job_type"], item["salary"]))
        self.conn.commit()
        #create job description
        sql_con.execute('INSERT INTO oc_t_item_job_description_attr(fk_i_item_id, fk_c_locale_code, s_desired_exp, s_studies, s_minimum_requirements, s_desired_requirements, s_contract, s_company_description) VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s")' % (fk_i_item_id, item["fk_c_locale_code"], item["s_desired_exp"], item["s_studies"], item["s_minimum_requirements"], item["s_desired_requirements"], item["s_contract"], item["s_company_description"]))
        self.conn.commit()
      else:
        pk_i_id = pk_i_id[0]
        #self.update_item(item, pk_i_id)
        sql_con.execute('UPDATE oc_t_item_job_attr SET s_company_name="%s",e_position_type="%s",s_salary_text="%s" WHERE fk_i_item_id=%s' % (item["name"], item["job_type"], item["salary"], pk_i_id))
        self.conn.commit()
      a = 5 
    except:
      print ":"
    return item

  def process_classifieds(self, item):
    try:
      sql_con = self.cursor
      sql_con.execute('SELECT fk_i_item_id FROM oc_t_item_description WHERE s_title="%s"' % item["title"])
      pk_i_id = sql_con.fetchone()
      if pk_i_id is None:
        fk_i_item_id = self.create_item(item)
      else:
        pk_i_id = pk_i_id[0]
        #self.update_item(item, pk_i_id)
    except:
      print ":"
    return item

  def process_house(self, item):
    try:
      sql_con = self.cursor
      sql_con.execute('SELECT fk_i_item_id FROM oc_t_item_description WHERE s_title="%s"' % item["title"])
      pk_i_id = sql_con.fetchone()
      if pk_i_id is None:
        #create item
        fk_i_item_id = self.create_item(item)
        #create house description
        fk_c_locale_code = "en_US"
        sql_con.execute('INSERT INTO oc_t_item_house_description_attr(fk_i_item_id, fk_c_locale_code, s_transport, s_zone) VALUES ("%s", "%s", NULL, NULL)' % (fk_i_item_id, fk_c_locale_code))
        self.conn.commit()
        #create house property
        # sql_con.execute('INSERT INTO oc_t_item_house_property_type_attr(fk_c_locale_code, s_name) VALUES ("%s", "%s")' % (fk_c_locale_code, item["name"]))
        # self.conn.commit()
        # sql_con.execute('SELECT LAST_INSERT_ID()')
        # fk_i_property_type_id = sql_con.fetchone()[0]
        fk_i_property_type_id = ""
        #create house
        sql_con.execute('INSERT INTO oc_t_item_house_attr(fk_i_item_id, s_square_meters, i_num_rooms, i_num_bathrooms, e_type, e_status, i_num_floors, i_num_garages, b_heating, b_air_condition, b_elevator, b_terrace, b_parking, b_furnished, b_new, b_by_owner, s_condition, i_year, s_agency, i_floor_number, i_plot_area, fk_i_property_type_id) VALUES ("%s", "%s", "%s", "%s", NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "%s", NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "%s")' % (fk_i_item_id, item["square_feet"], item["bedroom"], item["full_baths"], item["parking"], fk_i_property_type_id))
        self.conn.commit()
      else:
        pk_i_id = pk_i_id[0]
        #self.update_item(item, pk_i_id)
        sql_con.execute('UPDATE oc_t_item_house_attr SET s_square_meters="%s",i_num_rooms="%s",i_num_bathrooms="%s",e_type=NULL,e_status=NULL,i_num_floors=NULL,i_num_garages=NULL,b_heating=NULL,b_air_condition=NULL,b_elevator=NULL,b_terrace=NULL,b_parking=NULL,b_furnished=NULL,b_new=NULL,b_by_owner=NULL,s_condition=NULL,i_year=NULL,s_agency=NULL,i_floor_number=NULL,i_plot_area=NULL WHERE fk_i_item_id=%s' % (item["square_feet"],item["bedroom"],item["full_baths"], pk_i_id))
        self.conn.commit()
    except:
      print ""
    return item

  #Get the corresponding id for each of make, made and vehicle  
  #if the corresponding id exists, return it
  #else create new one and return
  def get_corresponding_id_for_car(self, sql_con, item, table_name, field, attr=None):
    if item[field] is not "":
      sql_con.execute('SELECT pk_i_id FROM %s WHERE s_name="%s"  LIMIT 1' % (table_name, item[field]))
      return_id = sql_con.fetchone()
      if return_id is None:
        sql = 'INSERT INTO %s (s_name) VALUES ("%s")' % (table_name, item[field])
        if field == "model":
          sql = 'INSERT INTO %s (fk_i_make_id, s_name) VALUES ("%s","%s")' % (table_name, attr["fk_i_make_id"], item[field])
        if field == "body_style":
          sql = 'INSERT INTO %s (fk_c_locale_code, s_name) VALUES ("en_US","%s")' % (table_name, item[field])
        sql_con.execute(sql)
        self.conn.commit()
        sql_con.execute('SELECT LAST_INSERT_ID()')
        return_id = sql_con.fetchone()[0]
      else:
        return_id = return_id[0]
      return return_id
    else:
      return ""

  #Validate the var
  def _f(self, var):
    if var is None:
      return ""
    else:
      return var

  # Change directories - create if it doesn't exist
  def chdir(self, _ftp, dir): 
    if self.directory_exists(_ftp, dir) is False: # (or negate, whatever you prefer for readability)
      _ftp.mkd(dir)
    _ftp.cwd(dir)

  # Check if directory exists (in current location)
  def directory_exists(self, _ftp, dir):
    filelist = []
    _ftp.retrlines('LIST',filelist.append)
    for f in filelist:
      if f.split()[-1] == dir and f.upper().startswith('D'):
          return True
    return False

  def get_last_folder_num_and_count(self, _ftp, dir_name):
    filelist = []
    _ftp.cwd(dir_name)
    _ftp.retrlines('LIST', filelist.append)
    num_list = re.findall("\d+", " ".join(map((lambda x: x.split(" ")[-1]), filelist)))
    if len(num_list) > 0:
      folder_num = max(num_list)
      _ftp.cwd(dir_name + "/%s" % folder_num)
      filelist = []
      _ftp.retrlines('LIST', filelist.append)
      file_count = len(filelist)
      return int(folder_num), file_count
    else:
      return -1, 0

  def get_new_folder_num(self, _ftp, dir_name):
    last_folder_num, count = self.get_last_folder_num_and_count(_ftp, dir_name)
    folder_num = last_folder_num  
    if last_folder_num == -1:
      folder_num = 0
    elif last_folder_num < 9:
      folder_num += (count + 1) / 100
    elif last_folder_num == 9:
      folder_num += (count + 1) / 100 * 2
    elif last_folder_num < 99:
      folder_num += (count + 1) / 1000 
    elif last_folder_num == 99:
      folder_num += (count + 1) / 1000 * 2 
      return folder_num, file_num      
    elif last_folder_num < 999:
      folder_num += (count + 1) / 10000
    elif last_folder_num == 999:
      folder_num += (count + 1) / 10000 * 2 
    return folder_num
     
  def get_category_id(self, category_str):
    if category_str == 'Cars':
      category_id = 31
    elif '-' in category_str:
      category_id = category_ids[category_str.split('-')[1].strip()]
    elif "for Rent" in category_str:
      category_id = category_ids[category_str.split('for')[0].strip()][0]
    elif "for Sale" in category_str:
      category_id = category_ids[category_str.split('for')[0].strip()][1]
    else:
      category_id = category_ids[category_str]
    if category_id == '3':
      category_id = 95
    if category_id is not None:
      return category_id
    sql_con = self.cursor
    sql_con.execute('SELECT fk_i_category_id, s_name FROM oc_t_category_description where fk_c_locale_code="en_US"')    
    category_list = sql_con.fetchall()
    category_id = ""
    if "for Rent" in category_str or "for Sale" in category_str:
      if "for Rent" in category_str:
        category_id = 44
        if category_str.startswith("Apartment"):
          category_id = 99
        if category_str.startswith("Commercial"):
          category_id = 50
        if category_str.startswith("Villa"):
          categoory_id = 100
      else:
        category_id = 43
        if category_str.startswith("Apartment"):
          category_id = 96
        if category_str.startswith("Commercial"):
          category_id = 98
        if category_str.startswith("Villa"):
          categoory_id = 97
    else:
      category_id = "1"
      for key, category in category_list:
        if map((lambda x:x.strip().upper()),category.split("-") )[0] in category_str.upper():
          category_id = key
          break
    return category_id