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
