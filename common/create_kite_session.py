from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import json
import time
import logging
from kiteconnect import KiteConnect
import urllib.parse as urlparse
from urllib.parse import parse_qs

logging.basicConfig(level=logging.INFO)


import common.application as ap
app_properties=ap.app_properties
api_key = app_properties['api_key']
api_secret = app_properties['api_secret']
user_name=app_properties["user_name"]
password=app_properties["password"]
pin=app_properties["pin"]
def get_session():
    kite = KiteConnect(api_key,api_secret)
    url = kite.login_url()
    driver = webdriver.Chrome()
    driver.get(url)
    time.sleep(2)
    login_form = driver.find_element_by_css_selector('div.login-form')
    formElements = login_form.find_elements_by_class_name('su-input-group')
    userElement = formElements[0]
    userIdInput = userElement.find_element_by_css_selector('input')
    userIdInput.send_keys(user_name)
    passwordElement = formElements[1]
    passwordInput = passwordElement.find_element_by_css_selector('input')
    passwordInput.send_keys(password)
    login_div = login_form.find_element_by_class_name('actions')
    submitButton = login_div.find_element_by_css_selector('button')
    submitButton.click()
    time.sleep(2)
    pinElement = driver.find_element_by_class_name('su-input-group')
    pinInput = pinElement.find_element_by_css_selector('input')
    pinInput.send_keys(pin) 
    login_div = login_form.find_element_by_class_name('actions')
    submitButton = login_div.find_element_by_css_selector('button')
    submitButton.click()
    time.sleep(2)
    url = driver.current_url
    parsed = urlparse.urlparse(url)
    request_token = parse_qs(parsed.query)['request_token'][0]
    data = kite.generate_session(request_token,api_secret)
    access_token = data["access_token"]
    kite.set_access_token(access_token)
    return kite