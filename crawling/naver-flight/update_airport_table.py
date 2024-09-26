from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import psycopg2
import os


load_dotenv('../resources/secret.env')
db_config = {
    "database": 'my_project_1',
    "host": os.getenv('DB_HOST'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    'port': os.getenv('DB_PORT')
}
conn = psycopg2.connect(**db_config)
cursor = conn.cursor()

chrome_options = Options()
chrome_options.add_argument("--headless")  # 백그라운드에서 실행
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)
driver.get("https://flight.naver.com/") # 네이버 항공권 main 페이지 접속
driver.implicitly_wait(10)

# click airport selection button
airport_select_button = driver.find_element(By.XPATH, "/html/body/div/div/main/div[2]/div/div/div[2]/div[1]/button[1]")
airport_select_button.click()
driver.implicitly_wait(3)

# click airport list buttons
airport_list_buttons_xpath = "/html/body/div/div/main/div[8]/div[2]/section/section/button"
airport_list_buttons = driver.find_elements(By.XPATH, airport_list_buttons_xpath)
for button in airport_list_buttons:
    button.click()
    driver.implicitly_wait(1)

for i in range(1, len(airport_list_buttons) + 1):
    airport_buttons_xpath = f'/html/body/div/div/main/div[8]/div[2]/section/section/div[{i}]/button'
    airport_buttons = driver.find_elements(By.XPATH, airport_buttons_xpath)
    for j in range(1, len(airport_buttons)+1):
        i_xpath = f'/html/body/div/div/main/div[8]/div[2]/section/section/div[{i}]/button[{j}]/span/i'
        i_elements = driver.find_elements(By.XPATH, i_xpath)
        city_and_country = i_elements[0].text.replace(' ', '')
        city, country = city_and_country.split(',')
        airport_code = i_elements[1].text

        # Check that there is iata_code or not
        cursor.execute("SELECT kor_city_name FROM my_project_1.raw_data.airport WHERE iata_code = %s", (airport_code,))
        result = cursor.fetchone()
        if result:
            cursor.execute("UPDATE my_project_1.raw_data.airport SET kor_city_name = %s WHERE iata_code = %s", (city, airport_code))
            print(f"Updated: {airport_code} -> {city}")

conn.commit()
cursor.close()
conn.close()

driver.quit()