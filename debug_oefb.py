"""Prüft wo die ÖFB-Bundesligen verlinkt sind."""
import time, re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
driver.set_page_load_timeout(60)

# Versuche die bekannte ÖFB-Frauen-Bundesliga-Spielplan-URL direkt
pages_to_check = [
    "https://www.oefb.at/oefb/Bewerb/Spielplan/226894?ADMIRAL-Frauen-Bundesliga-Grunddurchgang",
    "https://www.oefb.at/oefb/Bewerb/226894?ADMIRAL-Frauen-Bundesliga-Grunddurchgang",
]

for url in pages_to_check:
    driver.get(url)
    time.sleep(4)
    print(f"\n=== {url} ===")
    print(f"Actual URL: {driver.current_url}")
    print(f"Title: {driver.title}")

    # Gibt es hier Dropdowns?
    for cls in ["DS-verband", "DS-gruppe", "DS-bewerb"]:
        els = driver.find_elements(By.CSS_SELECTOR, f"ul.{cls} li a")
        if els:
            print(f"\n  ul.{cls}: {len(els)} Einträge")
            for el in els:
                t = el.get_attribute("title") or el.text or ""
                u = el.get_attribute("data-url") or ""
                print(f"    {t} -> {u}")

    # Filter div?
    filters = driver.find_elements(By.CSS_SELECTOR, "div.filter")
    if filters:
        print(f"\n  div.filter gefunden! Inner text (200 chars):")
        print(f"  {filters[0].text[:300]}")

    # Source nach data-url durchsuchen
    source = driver.page_source
    data_urls = re.findall(r'data-url="([^"]+)"', source)
    if data_urls:
        unique = sorted(set(data_urls))
        print(f"\n  data-url im Source: {len(unique)}")
        for m in unique[:20]:
            print(f"    {m}")

driver.quit()
