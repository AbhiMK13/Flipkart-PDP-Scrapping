import scrapy as sp
import pandas as pd
from scrapy.crawler import CrawlerProcess
import datetime
from urllib.parse import urlparse
import time
import re
from twisted.internet import reactor, defer
from scrapy.utils.log import configure_logging
from scrapy.crawler import CrawlerRunner
# import google.cloud.storage
from google.cloud import storage
import json
import os
import sys

#Directory of this python code
BASE_DIR = os.path.dirname(os.path.abspath('__file__'))

#Directories of the input,Exception and output folder through OS
input_dir = os.path.join(BASE_DIR + "\\Input")
error_dir = os.path.join(BASE_DIR + "\\Exception")
output_dir = os.path.join(BASE_DIR + "\\OutputData")
no_filter_dir = os.path.join(BASE_DIR + '\\NoFilteredData')


trying_urls = []

#Url for generating product Id
main_url = 'https://www.flipkart.com/x/p/k?pid='
# df_product_id = pd.read_excel("New_Latest_Input_platformwise.xlsx", sheet_name="Flipkart")#Input file
urls,output_data = [],[]
# for id in df_product_id['product_id']: urls.append(main_url + str(id))#Generating Urls
df_xpaths = pd.read_excel('xpath_flipkart_pdp.xlsx')#Xpath file

#Global Variable to strore the data 

main_output_lis = []
Exception_ProductID = []


#this funtion extracts all data and generated mandatory, non mandatory and error Data for further process
def parse(response):
        #this try block will initially clear all the error Id's if there are some
        try: 
            if(len(pd.read_excel(r"{}\\FlipkartPDP_Exception_ProductID.csv".format(error_dir)))>0): pd.DataFrame({"Exception_Id":[]}).to_excel(r"{}\\FlipkartPDP_Exception_ProductID.xlsx".format(error_dir))
        except: pass
        global trying_urls,Exception_ProductID,main_output_lis
        pass
        producId = response.url.split(main_url)[1]
        temp_dict = {}
        #The below data will be static for all the product ID.
        temp_dict.update({"Date": datetime.date.today().strftime("%Y-%m-%d")})
        today = datetime.date.today()
        temp_dict.update({"Week": today.isocalendar()[1]})
        temp_dict.update({"Marketplace": urlparse(response.url).netloc.replace('www.','').replace('.com','').title()})
        temp_dict.update({"Product ID": response.url[response.url.index('pid=')+4:]})
        temp_dict.update({"Product URL": response.url})

        #This try block will get the data for the coulmns specified in Xpath excel
        try:
                #This will loop the Xpath file to get the column name and xpath written in file to get the desired data
                for key in range(len(df_xpaths)):
                    if(df_xpaths['name'][key]=='MRP'):
                        val = ''
                        for v in response.xpath((df_xpaths['xpath'][key] + '/text()')).getall(): val += v
                        temp_dict.update({df_xpaths['name'][key] : val if val!='' and val!=None else ''})
                    elif(df_xpaths['name'][key]=='Seller_Offers'): 
                        temp_lis = []
                        for val in response.xpath((df_xpaths['xpath'][key] + '/text()')).getall(): temp_lis.append(val)
                        temp_dict.update({df_xpaths['name'][key] : temp_lis if len(temp_lis)>0 else ''})
                    elif(df_xpaths['name'][key]=='In Stock'):
                        temp_dict.update({df_xpaths['name'][key] : 'In Stock' if response.xpath((df_xpaths['xpath'][key] + '/text()')).get()==None else 'Out Of Stock'})
                    elif(df_xpaths['name'][key]=='COD'):
                        temp_dict.update({df_xpaths['name'][key] : 'Yes' if response.xpath((df_xpaths['xpath'][key] + '/text()')).get()!=None else 'No'})
                    elif(df_xpaths['name'][key]=='Image URL'):
                        val = response.xpath(df_xpaths['xpath'][key]).get()
                        temp_dict.update({df_xpaths['name'][key]:val})
                    elif(df_xpaths['name'][key]=='Offers'):
                        val = response.xpath((df_xpaths['xpath'][key] + '/text()')).getall()
                        temp_dict.update({"No. of Offers": len(val)})
                        temp_dict.update({df_xpaths['name'][key] : [val] if len(val)>0 else ''})
                    elif(df_xpaths['name'][key]=='Count of Ratings'):
                        try:
                            val = response.xpath((df_xpaths['xpath'][key] + '/text()')).get()
                            val = val.split("and")
                            temp_dict.update({df_xpaths['name'][key] :  int(re.search(r'\d+', val[0]).group()) if len(val)>0 else ''})
                            temp_dict.update({"Count of Reviews" : int(re.search(r'\d+', val[1]).group()) if len(val)>1 else ''})
                        except: 
                            temp_dict.update({df_xpaths['name'][key] : ''})
                            temp_dict.update({"Reviews" : ''})
                    elif(df_xpaths['name'][key]=='Total Sizes'):
                        val = response.xpath((df_xpaths['xpath'][key] + '/text()')).getall()
                        temp_dict.update({"No of Sizes": len(val)})
                        temp_dict.update({df_xpaths['name'][key] : [val] if len(val)>0 else ''})
                    elif(df_xpaths['name'][key]=='Available Sizes'):
                        val = response.xpath((df_xpaths['xpath'][key] + '/text()')).getall()
                        temp_dict.update({"No of Available Sizes": len(val)})
                        temp_dict.update({"Available Sizes": [val] if len(val)>0 else ''}) 
                        temp_dict.update({"No of Non-Available Sizes": len([x for x in temp_dict['Total Sizes'][0] if x not in temp_dict['Available Sizes'][0]])})
                        temp_dict.update({"Non-Available Sizes": [[x for x in temp_dict['Total Sizes'][0] if x not in temp_dict['Available Sizes'][0]]]})          
                    elif(df_xpaths['name'][key]=='Available Colors'):
                        val = response.xpath((df_xpaths['xpath'][key])).extract()
                        temp_dict.update({"No of Colors": len(val)})
                    elif(df_xpaths['name'][key]=='No of Images'):
                        val = response.xpath((df_xpaths['xpath'][key])).getall()
                        temp_dict.update({"No of Images": len(val) if len(val)>0 else ''})
                    elif(df_xpaths['name'][key]=='Product Details'):
                        val = response.xpath((df_xpaths['xpath'][key] + '/text()')).getall()
                        temp_dict.update({"Product Details": dict(zip(val[0::2],val[1::2])) if len(val)>0 else ''})
                        try: 
                            temp_dict.update({"Fabric":temp_dict['Product Details']['Fabric'] if(temp_dict['Product Details']['Fabric']!=None) else ''})
                            temp_dict.update({"Bestseller Rank": ''})
                            temp_dict.update({"Rank Detail": ''})
                            temp_dict.update({"Ques": ''})
                        except: temp_dict.update({"Fabric":''})
                    elif(df_xpaths['name'][key]=='Questions_Answers'): pass
                    elif(df_xpaths['name'][key]=='BuyNow'):
                        temp_dict.update({df_xpaths['name'][key] : 'Yes' if response.xpath((df_xpaths['xpath'][key] + '/text()')).get()!=None else 'No'})

                    else: 
                        val = response.xpath((df_xpaths['xpath'][key] + ('/text()' if df_xpaths['name'][key]!='Image_URL' else ''))).get()
                        temp_dict.update({df_xpaths['name'][key] : val if val!='' and val!=None else ''})
                

        #If there is no data for particular tag then data black will be aligned for particular ccolumn
        except:
            temp_dict.update({df_xpaths['name'][key]: ''})

        #The below condition checks if the specified columns are empty if yes then InStock will be "Out of Stock"
        if(temp_dict['Title'] == '' and temp_dict['Brand']=='' and temp_dict['Division']==''):
                        print("\n\n",temp_dict['In Stock'])
                        temp_dict.update({'In Stock':'Out Of Stock'})
                        print(temp_dict['In Stock'])

        #The below line appends all the data either mandatory or non-mandatory 
        output_data.append(temp_dict)


        #This is the condition to check the data mandatory or non mandatory
        if(temp_dict['Title']!='' and temp_dict['MRP']!='' and temp_dict['COD']!= 'No'
             and temp_dict['Brand']!= '' and temp_dict['In Stock']!='Out Of Stock' and temp_dict['Image URL']!= ''
              and temp_dict['Selling Price'] != '' and temp_dict['Count of Ratings']!='' and temp_dict['Product Rating']!= '' ):
            #Appends mandatory data for main_output_lis
            main_output_lis.append(temp_dict)
            print(main_output_lis)
            #Returns the mandatory data 
            return temp_dict

        else :  
            #Appends the error IDs
            Exception_ProductID.append(producId)
            trying_urls.append(response.url)

#First iteration for getting the data and error ID's
class FlipkartpdpSpider(sp.Spider):
    # Global Flow 
    name = 'flipkartpdp'
    allowed_domains = ['flipkart.com']
    #For generating urls for each product ID
    main_url = 'https://www.flipkart.com/x/p/k?pid='

    #Input file name along with sheetname to be decaled below
    df_product_id = pd.read_excel(r"{}\\BK_input_filev2.xlsx".format(input_dir), sheet_name="Flipkart")
    urls,output_data = [],[]

    #Presenting the Column name below
    for id in df_product_id['Portal_Id']: urls.append(main_url + str(id))
    df_xpaths = pd.read_excel('xpath_flipkart_pdp.xlsx')
    #Function to get the urls and sends the response to parse function
    def start_requests(self):
        try: 
            if(len(pd.read_excel(r"{}\\FlipkartPDP_Exception_ProductID.csv".format(error_dir)))>0): pd.DataFrame({"Exception_Id":[]}).to_excel(r"{}\\FlipkartPDP_Exception_ProductID.xlsx".format(error_dir),index=False)
        except: pass
        for url in self.urls:
            yield sp.Request(url, callback=parse)
 
# Scrapy Crawling Process   
process = CrawlerProcess(settings={'LOG_LEVEL': 'DEBUG',
                                   'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                                   'FEEDS': {"FlipkartPDP1.csv": {'format': 'csv', 'overwrite': True}}
                                   })
process.crawl(FlipkartpdpSpider)
Exception_ProductID = []

#Second Error ID iteration which takes input as Error ID generated from First itteration
class TryingSpider(sp.Spider):
  name = 'tryingurl'
  def start_requests(self):
        global trying_urls,Exception_ProductID
        #Iteration of Error ID
        for try_url in trying_urls:
            main_lis_len = len(main_output_lis)
            #Number of Itteration is sepcified in range(Num_of_Itteration)
            for looping in range(2):
               if(len(main_output_lis)>main_lis_len):break
               yield sp.Request(try_url, callback=parse)
      
#Scrapy crawling process for the second itteration                      
again_looping = CrawlerProcess(settings={'LOG_LEVEL': 'DEBUG',
                                   'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                                   'FEEDS': {"FlipkartPDP2.csv": {'format': 'csv', 'overwrite': True}}
                                   })
again_looping.crawl(TryingSpider)

# For Remove ReactModule Error In Scrapy
configure_logging()
runner = CrawlerRunner()
@defer.inlineCallbacks

#Calls the first itteration FlipkartpdpSpider class and then the second one TryingSpider
def crawl():
    yield runner.crawl(FlipkartpdpSpider)
    yield runner.crawl(TryingSpider)
    reactor.stop()
crawl()
reactor.run()


# Filtering Data
original_df = pd.DataFrame(main_output_lis)
print("\n\n\n\n\n\n\n\n",original_df,"\n\n\n\n\n\n\n\n\n\n")

#Generates the error ID's
try:
    output_product_id = list(original_df['Product ID'])
    main_exp_id = []
    #if the Input Product IDs are not in Output list then those IDs are considered as Error IDs
    for exp_id in set(Exception_ProductID):
        Exception_ProductID = []
        if exp_id not in output_product_id:
            main_exp_id.append(exp_id)
    Exception_Df = pd.DataFrame({"Exception_ProductID":list(set(main_exp_id))}).to_csv(r"{}\\FlipkartPDP_Exception_ProductID.csv".format(error_dir))

except:
    Exception_Df = pd.DataFrame({"Exception_ProductID":list(set(Exception_ProductID))}).to_csv(r"{}\\FlipkartPDP_Exception_ProductID.csv".format(error_dir), index=False)



#Filteration of Data   
data_withoutFitered = pd.DataFrame(output_data)
data_withoutFitered.drop_duplicates(subset=['Product ID'],inplace=True)
data_withoutFitered.to_excel(r"{}\\Flipkart_NoFilteredData.xlsx".format(no_filter_dir), index=False)

data_withoutFitered.drop_duplicates(subset='Product ID', keep="last",inplace=True)
data_withoutFitered['Selling Price'] = data_withoutFitered['Selling Price'].str.replace("₹",'',regex=True)
data_withoutFitered['MRP'] = data_withoutFitered['MRP'].str.replace("₹",'',regex=True)
data_withoutFitered['Discount'] = data_withoutFitered['Discount'].str.replace('%','',regex=True).str.replace(" off",'',regex=True)
data_withoutFitered.drop_duplicates(subset='Product ID', inplace=True)

#All extracted data will be saved as output
data_withoutFitered.to_excel(r'{}\\Flipkart_All_Data.xlsx'.format(output_dir), index = False)#All data with and without mandatory data will be generated

try:
    original_df.drop_duplicates(subset='Product ID', keep="last",inplace=True)
    original_df['Selling Price'] = original_df['Selling Price'].str.replace("₹",'',regex=True)
    original_df['MRP'] = original_df['MRP'].str.replace("₹",'',regex=True)
    original_df['Discount'] = original_df['Discount'].str.replace('%','',regex=True).str.replace(" off",'',regex=True)
    #Mandatory data will be generated here
    original_df.to_excel(r"{}\\FlipkartPDP_Mandatory_Output.xlsx".format(output_dir),index=False)
except:
    pass


#Considering the All data and mandatory data Non-mandatory data will be generated in below code
all_data = pd.read_excel(r"{}\\Flipkart_All_Data.xlsx".format(output_dir))
mandatory_data = pd.read_excel(r"{}\\FlipkartPDP_Mandatory_Output.xlsx".format(output_dir))
mandatory_ids = mandatory_data['Product ID']
for prd_id in mandatory_ids:
    all_data = all_data[all_data['Product ID']!=prd_id]
all_data.drop_duplicates(subset='Product ID', inplace=True)
#Unique non mandatory data will be generated here
all_data.to_excel(r"{}\\FlipkartPDP_NonMandatory.xlsx".format(output_dir), index=False)


#Function to push the mandatory and non mandatory data into GCP


