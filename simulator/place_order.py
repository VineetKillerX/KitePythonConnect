import time

from selenium import webdriver

import common.application as ap

simulator_props = ap.simulator_props

## inputs
url = 'https://moneybhai.moneycontrol.com'
email = simulator_props['email']
pwd = simulator_props['pwd']


def login(email, pwd, counter=0):
    try:
        driver = webdriver.Firefox()
        driver.get(url)
        time.sleep(2)
        ## loaded page
        skipbtn = driver.find_element_by_class_name('skip')
        skipbtn.click()
        time.sleep(1)
        loginBtn = driver.find_element_by_id('loginbtn')
        loginBtn.click()
        time.sleep(1)
        driver.switch_to.frame(driver.find_element_by_id('myframe'))
        time.sleep(2)
        emailInput = driver.find_elements_by_id('email')
        emailInput[1].send_keys(email)
        pwdInput = driver.find_elements_by_id('pwd')
        pwdInput[1].send_keys(pwd)
        time.sleep(2)
        driver.find_element_by_id('ACCT_LOGIN_SUBMIT').click()
        time.sleep(2)
        ## logged in
        return driver
    except:
        print("Exception in login")
        counter = counter + 1
        if counter < 3:
            login(email, pwd, counter)


driver = login(email, pwd)


def place_order(action, stock, quantity, counter=0):
    try:
        driver.switch_to.default_content()
        url_transact = 'https://moneybhai.moneycontrol.com/neworderview'
        driver.get(url_transact)
        search = driver.find_element_by_class_name('search-input')
        search.send_keys(stock)
        time.sleep(2)
        driver.find_element_by_id('searchButton').click()
        time.sleep(2)
        auto = driver.find_element_by_id('auto-suggest')
        li1 = auto.find_element_by_css_selector('ul > li')
        li1.click()
        time.sleep(5)
        driver.find_elements_by_class_name('mb-select')[1].find_elements_by_css_selector('option')[1].click()
        if action == 'BUY':
            driver.find_element_by_class_name('buysellbtn').click()
        else:
            driver.find_element_by_id('stocksellBtn').click()
        time.sleep(2)
        driver.find_element_by_class_name('orderQty').send_keys(quantity)
        driver.find_element_by_id('btn_submit').click()
    except:
        print("Exception in ", action, " for Stock : ", stock, " And Quantity : ", quantity)
        counter = counter + 1
        if counter < 3:
            place_order(action, stock, quantity, counter)
