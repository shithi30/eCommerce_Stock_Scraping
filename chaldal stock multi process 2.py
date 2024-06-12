#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# import
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup
import pandas as pd
import duckdb
import multiprocessing
import win32com.client
from pretty_html_table import build_table
import time
from datetime import datetime


# In[ ]:


## Chaldal ##
def scrape_chaldal_process(brands): 
    
    # accumulators
    df_acc_local = pd.DataFrame()
    lock = multiprocessing.Lock()

    # open window
    driver = webdriver.Chrome('chromedriver', options=[])
    driver.maximize_window()
    wait = WebDriverWait(driver, 40)

    for b in brands:
        # url
        url = "https://chaldal.com/search/" + b
        driver.get(url)
        
        # scroll
        SCROLL_PAUSE_TIME = 5
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE_TIME)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height: break
            last_height = new_height

        # soup
        soup_init = BeautifulSoup(driver.page_source, 'html.parser')
        soup = soup_init.find_all("div", attrs={"class": "imageWrapper"})
        
        # scrape
        skus = []
        quants = []
        prices = []
        prices_if_discounted = []
        for s in soup:
            # sku
            try: val = s.find("div", attrs={"class": "name"}).get_text()
            except: val = None
            skus.append(val)
            # quantity
            try: val = s.find("div", attrs={"class": "subText"}).get_text()
            except: val = None
            quants.append(val)
            # price
            try: val = float(s.find("div", attrs={"class": "price"}).get_text().split()[1].replace(',', ''))
            except: val = None
            prices.append(val)
            # discount
            try: val = float(s.find("div", attrs={"class": "discountedPrice"}).get_text().split()[1].replace(',', ''))
            except: val = None
            prices_if_discounted.append(val)
        
        # accumulate
        df = pd.DataFrame()
        df['sku'] = skus
        df['brand'] = b
        df['quantity'] = quants
        df['price'] = prices
        df['price_if_discounted'] = prices_if_discounted
        
        # relevant data
        qry = '''
        select *
        from
            (select *, row_number() over() pos_in_pg
            from df
            ) tbl1 
        where sku ilike ''' + "'" + b + '''%';
        '''
        df = duckdb.query(qry).df()
        rel_idx = df['pos_in_pg'].tolist()
        len_rel_idx = len(rel_idx)
        
        # description
        descs = []
        for i in range(0, len_rel_idx): 
            descs.append(None)
            try:
                # move
                path = '//*[@id="page"]/div/div[6]/section/div/div/div/div/section/div[2]/div[2]/div['+str(rel_idx[i])+']/div/div'
                elem = driver.find_element(By.XPATH, path)
                mov = ActionChains(driver).move_to_element(elem)
                mov.perform()
                # details
                path1 = '//*[@id="page"]/div/div[6]/section/div/div/div/div/section/div[2]/div[2]/div['+str(rel_idx[i])+']/div/div/div[5]/span/a'
                path2 = '//*[@id="page"]/div/div[6]/section/div/div/div/div/section/div[2]/div[2]/div['+str(rel_idx[i])+']/div/div/div[6]/span/a'
                try: elem = driver.find_element(By.XPATH, path1)
                except: elem = driver.find_element(By.XPATH, path2)
                elem.click()
                # content
                path = '//*[@id="page"]/div/div[6]/section/div/div/div/div/section/div[2]/div[2]/div['+str(rel_idx[i])+']/div/div[2]/div/div/article/section[2]/div[5]'
                elem = driver.find_element(By.XPATH, path)
                descs[i] = elem.text.replace("\n", " ")
                # close
                path = '//*[@id="page"]/div/div[6]/section/div/div/div/div/section/div[2]/div[2]/div['+str(rel_idx[i])+']/div/div[2]/div/button'
                elem = driver.find_element(By.XPATH, path)
                elem.click()
            except: pass
        # progress
        lock.acquire()
        print("Descriptions fetched for: " + b)
        lock.release()
        
        # stock
        stocks = []
        report_times = []
        for i in rel_idx: 
            stk = 0
            try: 
                # add to bag
                path = '//*[@id="page"]/div/div[6]/section/div/div/div/div/section/div[2]/div[2]/div['+str(i)+']/div/section'
                elem = driver.find_element(By.XPATH, path)
                clks = 1
                while(1): 
                    mov = ActionChains(driver).move_to_element(wait.until(EC.element_to_be_clickable(elem)))
                    for j in range (0, clks): mov.click().perform()
                    # check unavailability
                    path = '//*[@id="page"]/div/div[6]/section/div/div/div/div/section/div[2]/div[2]/div['+str(i)+']/div/section/p'
                    try: 
                        # read bag
                        stk = int(elem.text.split()[0].replace(",", ""))
                        elem = driver.find_element(By.XPATH, path)
                        break
                    except: pass
                    # add more to bag
                    path = '//*[@id="page"]/div/div[6]/section/div/div/div/div/section/div[2]/div[2]/div['+str(i)+']/div/div[2]/div'
                    try: elem = driver.find_element(By.XPATH, path)
                    except: break
                    clks = 100
            except: stk = None
            # report bag
            lock.acquire()
            print("Stock for " + skus[i-1] + " " + quants[i-1] + ": " + str(stk))
            lock.release()
            stocks.append(stk)
            report_times.append(time.strftime('%Y-%m-%d %H:%M:%S'))
    
        # accumulate
        df['stock'] = stocks
        df['description'] = descs
        df['report_time'] = report_times
        df_acc_local = df_acc_local.append(df)
        
    # close window
    driver.close()
    
    # return
    return df_acc_local
    
def scrape_chaldal():
    
    # accumulators
    start_time = time.time()
    brands = ['Boost', 'Clear', 'Simple', 'Pepsodent', 'Brylcreem', 'Bru', 'St. Ives', 'Horlicks', 'Sunsilk', 'Lux', 'Pond', 'Closeup', 'Cif', 'Dove', 'Maltova', 'Domex', 'Clinic', 'Tresemm', 'GlucoMax', 'Knorr', 'Glow & Lovely', 'Glow & Handsome', 'Wheel', 'Axe', 'Pureit', 'Lifebuoy', 'Surf Excel', 'Vaseline', 'Vim', 'Rin']
    process_count = 3
    brands_chunks = [brands[i::process_count] for i in range(process_count)]
    
    # processes
    pool = multiprocessing.Pool(process_count)
    dfs_acc = pool.map(scrape_chaldal_process, brands_chunks)
    pool.close()
    pool.join()

    # csv
    df_acc = pd.DataFrame()
    for i in range(0, process_count):
        df_acc = df_acc.append(dfs_acc[i])
    folder = r"C:\\Users\\Shithi.Maitra\\Unilever Codes\\Scraping Scripts\\Chaldal Stocks\\"
    filename = folder + "chaldal_unilever_stocks_data_" + datetime.today().strftime('%Y-%m-%d') + ".csv"
    df_acc.to_csv(filename, index=False)

    # analysis
    qry = '''
    select * 
    from 
        (select 
            'Chaldal' platform, 
            count(sku) "SKUs", 
            count(case when stock=0 then sku else null end) "SKUs out of stock", 
            count(case when stock is null then sku else null end) "SKUs failed to scrape stock",
            ''' + str(len(brands)) + '''-count(distinct brand) "brands failed to scrape stock",
            count(case when length(description)=0 then sku else null end) "SKUs not described",
            min(strptime(report_time, '%Y-%m-%d %H:%M:%S')) "stocking start time",
            right(age(max(strptime(report_time, '%Y-%m-%d %H:%M:%S')), min(strptime(report_time, '%Y-%m-%d %H:%M:%S'))), 8) "time to scrape stocks"
        from df_acc
        ) tbl1,

        (select 
            brand "longest stock to scrape", 
            right(age(max(strptime(report_time, '%Y-%m-%d %H:%M:%S')), min(strptime(report_time, '%Y-%m-%d %H:%M:%S'))), 8) "longest time to scrape stock"
        from df_acc
        group by 1
        order by 2 desc
        limit 1
        ) tbl2; 
    '''
    res_df = duckdb.query(qry).df()
    
    # stats
    print("\nTotal SKUs found: " + str(df_acc.shape[0]))
    elapsed_time = str(round((time.time() - start_time) / 60.00, 2))
    print("Elapsed time to run script (mins): " + elapsed_time)
    
    return res_df


# In[ ]:


# email
def send_email(smry_df):
    
    # object
    ol = win32com.client.Dispatch("outlook.application")
    olmailitem = 0x0
    newmail = ol.CreateItem(olmailitem)

    # subject, recipients
    newmail.Subject = 'Chaldal Stocks ' + time.strftime('%d-%b-%y')
    # newmail.To = 'shithi.maitra@unilever.com'
    newmail.To = 'mehedi.asif@unilever.com'
    # newmail.CC = 'mehedi.asif@unilever.com; zakeea.husain@unilever.com; rakaanjum.unilever@gmail.com; nazmussajid.ubl@gmail.com'

    # body
    newmail.HTMLbody = f'''
    Dear concern,<br><br>
    Today's <i>Chaldal</i> stocks for Unilever SKUs have been scraped. A brief statistics of the process is given below:
    ''' + build_table(smry_df, 'blue_light') + '''
    Please find the data attached. Note that, this email was auto generated at ''' + time.strftime('%d-%b-%y, %I:%M %p') + ''' using <i>win32com</i>.<br><br>
    Thanks,<br>
    Shithi Maitra<br>
    Asst. Manager, Cust. Service Excellence<br>
    Unilever BD Ltd.<br>
    '''

    # attachment(s) 
    folder = r"C:\\Users\\Shithi.Maitra\\Unilever Codes\\Scraping Scripts\\Chaldal Stocks\\"
    filename = folder + "chaldal_unilever_stocks_data_" + datetime.today().strftime('%Y-%m-%d') + ".csv"
    newmail.Attachments.Add(filename)

    # display, send
    # newmail.Display()
    newmail.Send()


# In[ ]:


# summary
if __name__ == "__main__":
    smry_df = scrape_chaldal()
    send_email(smry_df)
    print(smry_df)


# In[ ]:


# run
# "C:\Users\Shithi.Maitra\Unilever Codes\Scraping Scripts\chaldal_stock_multiprocess.py"


# In[15]:


# import
import pandas as pd
import duckdb
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup
from googleapiclient.discovery import build
from google.oauth2 import service_account
import time

# accumulators
start_time = time.time()
df = pd.DataFrame()

# preference
options = webdriver.ChromeOptions()
options.add_argument('ignore-certificate-errors')

# open window
driver = webdriver.Chrome(options=options)
driver.maximize_window()

# url
url = "https://chaldal.com/Unilever"
driver.get(url)

# scroll
scroll_pause_time = 5
last_height = driver.execute_script("return document.body.scrollHeight")
while True:
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(scroll_pause_time)
    new_height = driver.execute_script("return document.body.scrollHeight")
    if new_height == last_height: break
    last_height = new_height

# soup
soup_init = BeautifulSoup(driver.page_source, 'html.parser')
soup = soup_init.find_all("div", attrs={"class": "product"})

# product
skus = []
quants = []
sku_count = len(soup)
for i in range(0, sku_count):
    
    # sku
    try: val = soup[i].find("div", attrs={"class": "name"}).get_text()
    except: val = None
    skus.append(val)
    # quantity
    try: val = soup[i].find("div", attrs={"class": "subText"}).get_text().replace(" ", "")
    except: val = None
    quants.append(val)

# stock
brands = ['Surf', 'Rin', 'Lux', 'Lifebuoy', 'Dove', 'Sunsilk', 'Clear', 'Tresemm', 'Clinic Plus', 'Closeup', 'Pepsodent']
stocks = []
for i in range(0, sku_count): 
    
    # portfolio
    pfolio = 0
    for b in brands: 
        if b in skus[i]: pfolio = 1
    if pfolio == 0: 
        stocks.append(None)
        continue
    
    # add to bag
    path = '//*[@id="page"]/div/div[6]/section/div/div/div/div/section/div[2]/div/div[' + str(i+1) + ']/div/section'
    elem = driver.find_element(By.XPATH, path)
    mov = ActionChains(driver).move_to_element(elem)
    mov.click().perform()
    
    # availability
    while(1):
        oos = 1
        path = '//*[@id="page"]/div/div[6]/section/div/div/div/div/section/div[2]/div/div[' + str(i+1) + ']/div/section'
        try: elem = driver.find_element(By.XPATH, path)
        except: oos = 0
        if oos == 1: break
        
        # add more
        path = '//*[@id="page"]/div/div[6]/section/div/div/div/div/section/div[2]/div/div[' + str(i+1) + ']/div/div[2]/div'
        elem = driver.find_element(By.XPATH, path)
        mov = ActionChains(driver).move_to_element(elem)
        for j in range(0, 100): mov.click().perform()
        
    # read
    path = '//*[@id="page"]/div/div[6]/section/div/div/div/div/section/div[2]/div/div[' + str(i+1) + ']/div/div[2]/div'
    elem = driver.find_element(By.XPATH, path)
    stocks.append(elem.text.split()[0])
    print("Stock for " + skus[i] + " " + quants[i] + ": " + str(stocks[i]))

# accumulate
df['sku'] = [str(s) + ' ' + str(q) for s, q in zip(skus, quants)]
df['stock'] = stocks
df['location'] = driver.find_element(By.CLASS_NAME, "metropolitanAreaName").text.replace("\n", " ")
df['report_time'] = time.strftime('%d-%b-%y, %I:%M %p')
df = duckdb.query('''select * from df where stock is not null''').df()

# close window
driver.close()

# credentials
SERVICE_ACCOUNT_FILE = 'read-write-to-gsheet-apis-1-04f16c652b1e.json'
SAMPLE_SPREADSHEET_ID = '1gkLRp59RyRw4UFds0-nNQhhWOaS4VFxtJ_Hgwg2x2A0'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# APIs
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

# extract
values = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Stocks!A1:D').execute().get('values', [])
df_prev = pd.DataFrame(values[1:] , columns = values[0])
# transform
qry = '''select * from df union all select * from df_prev'''
df_pres = duckdb.query(qry).df()
# load
sheet.values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='Stocks').execute()
sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="'Stocks'!A1", valueInputOption='USER_ENTERED', body={'values': [df_pres.columns.values.tolist()] + df_pres.fillna('').values.tolist()}).execute()
   
# stats
display(df.head(5))
print("Listings in result: " + str(df.shape[0]))
print("Elapsed time to report (mins): " + str(round((time.time() - start_time) / 60.00, 2)))


# In[ ]:




