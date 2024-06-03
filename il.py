import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

# import urllib.request
from urllib.request import urlopen
import json
import requests
import numpy as np
import plotly.express as px
from json import loads
from sqlalchemy import create_engine
import os
import time
import subprocess
import warnings
warnings.filterwarnings('ignore')

url = "https://www.isbe.net/ilreportcarddata"

download_dir = r'.'
options = Options()
options.add_argument("--headless")
options.add_argument("--disable-gpu")
options.add_argument("--disable-extensions")
options.add_experimental_option("prefs", {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})



# Initialize the Chrome WebDriver
driver = webdriver.Chrome(options=options)
driver.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": 'Chrome'})

#pull all the links on the report card page
elements  =  driver.find_elements(By.TAG_NAME,'a')

#initialize empty list of links
links = []

#for each element that is labeled as a report card dataset, click the link to download
for element in elements:
    href = element.get_attribute("href")
    if href and (".xls" in href)  and (("RC-Pub-Data-Set" in href) or ("Report-Card-Public-Data-Set" in href)):
        links.append(href)
        print(href)


for link in links:
    driver.get(link)
    time.sleep(15)  # Allow some time for the download to complete
# Close the WebDriver
driver.quit()
print(f"Downloaded {len(links)} XLS files to {download_dir}")
#files succesfully downloaded to folder

#set the directory
il_files = os.listdir(r'C:\Users\Sam Koenig\Desktop\public_data\il_data')

#start transforming the publically available data
def il_metrics_transform():
    print("Running Illinois Code:")
    df_final = pd.DataFrame()
    for xls_file in il_files:
        if '.xls' in xls_file:
            df_general = pd.read_excel('.'+xls_file, 'General') #read the general tab on the xls file
            df_general['district_id'] = df_general['RCDTS'].str[:11] #create a field called district ID based on the field RCDTS (11 digits of the 15 digits RCTDS code is District ID)
            df_general = df_general.assign(file = xls_file) #create a column with the file name
            df_general['year'] = df_general['file'].str[:4].replace('23-R','2023') #replace bad file naming convention on illinois report card site
            df_district =  df_general.loc[(df_general.Type == "District")] #create a dataframe for just the district results
            df_school =  df_general.loc[(df_general['School Type'] == "HIGH SCHOOL")] #create a dataframe for just the high school results
            if 'RCDTS' in df_general.columns[0]: #ensure that it's the right file
                #### Freshman On Track####
                try:
                    df_fot = df_district[['district_id', 'Type', 'year', '% 9th Grade on Track', '# 9th Grade on Track']] #pull out district Id, District Type, Metric, and year
                    df_fot = df_fot.rename(columns={'Type': 'type', '% 9th Grade on Track': 'value','# 9th Grade on Track': 'numerator'}) #rename columns
                    df_fot = df_fot.assign(metric_name = 'Freshman On Track', metric_type = 'percent') #assign value of freshman on track to the metric_name and metric type
                except:
                    print("There is an error with FRESHMAN ON TRACK in the following file: " + xls_file) #print an error in case if file does not have column header name
                #### HIGH SCHOOL GRADUATION ####
                try:
                    df_grad_rate = df_district[['district_id', 'Type', 'year', 'High School 4-Year Graduation Rate - Total']] #pull out district Id, District Type, Metric, and year
                    df_grad_rate = df_grad_rate.rename(columns={'Type': 'type', 'High School 4-Year Graduation Rate - Total': 'value'}) #rename columns
                    df_grad_rate = df_grad_rate.assign(metric_name = 'High School Graduation Rate', metric_type = 'percent') #assign value of high school graduation to the metric_name and metric type
                except:
                    print("There is an error with HIGH SCHOOL GRADUATION in the following file: " + xls_file) #print an error in case if file does not have column header name
                #### 16 Month Enrollment ####
                try:
                    df_enrollment = df_district[['district_id', 'Type', 'year', '% Graduates enrolled in a Postsecondary Institution within 16 months']] #pull out district Id, District Type, Metric, and year
                    df_enrollment = df_enrollment.rename(columns={'Type': 'type', '% Graduates enrolled in a Postsecondary Institution within 16 months': 'value'}) #rename columns
                    df_enrollment = df_enrollment.assign(metric_name = '16 Month Postsecondary Enrollment', metric_type = 'percent') #assign value of to the metric_name, and metric type
                except:
                    print("There is an error with ENROLLMENT in the following file: " + xls_file) #print an error in case if file does not have column header name
                #### Attendance ####
                try:
                    df_att = df_school[['district_id', 'Type', 'year','Student Attendance Rate']] #pull out district Id, District Type, Metric, and year
                    df_att = df_att.rename(columns={'Type': 'type', 'Student Attendance Rate': 'value'}) #rename columns
                    df_att = df_att.groupby(['district_id']).agg({'district_id': 'max','type':'max','year':'max','value': 'mean'})
                    df_att = df_att.assign(metric_name = 'High School Attendance', metric_type = 'percent') #assign value of to the metric_name
                except:
                    print("There is an error with ATTTENDANCE in the following file: " + xls_file)#print an error in case if file does not have column header name
                #### CTE Enrollment ####
                try:
                    df_cte = df_district[['district_id', 'Type', 'year','# CTE Enrollment']] #pull out district Id, District Type, Metric, and year
                    df_cte = df_cte.rename(columns={'Type': 'type', '# CTE Enrollment': 'value'}) #rename columns
                    df_cte = df_cte.assign(metric_name = 'CTE Enrollment', metric_type = 'percent') #assign value of to the metric_name
                except:
                    print("There is an error with CTE in the following file: " + xls_file)#print an error in case if file does not have column header name
                df_final = pd.concat([df_final, df_fot, df_grad_rate, df_enrollment, df_att]) #append the dataframe to combined dataframe
                il_metrics_df = df_final.merge(df_all_partners,left_on ='district_id', right_on='rcdts')
                il_metrics_df = il_metrics_df.rename(columns={'NCES ID': 'district_nces_id'}) #rename nces id for eventual union
                il_metrics_df = il_metrics_df[['district_nces_id', 'district_id','type','value', 'numerator','metric_name','metric_type', 'year', 'Zip']] # pull only relevant data

    il_metrics_df.to_csv('il.csv', index= False) #download cleaned datafile to csv
