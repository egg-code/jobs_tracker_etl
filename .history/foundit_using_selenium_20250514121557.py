import json
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class FounditScraper:
    def __init__(self, headless=False):
        options = Options()
        options.add_argument("--start-maximized")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        if headless:
            options.add_argument("--headless=new")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")

        print("Launching browser.....Hello Sandi")
        self.driver = webdriver.Chrome(service=ChromeService(), options=options)
        self.driver.set_page_load_timeout(60)  # ⬅️ Set global page load timeout
        self.wait = WebDriverWait(self.driver, 15)
        self.base_url = "https://www.foundit.sg/srp/results?sort=1&limit=15&query=%22%22&quickApplyJobs=true&industries=information+technology%2Csoftware%2Csoftware+engineering%2Cit+management"
        self.limit = 15

    def _safe_get_text(self, parent, selector):
        try:
            return parent.find_element(By.CSS_SELECTOR, selector).text.strip()
        except NoSuchElementException:
            return ''


    def run(self):
        start = 0
        all_jobs = []

        while True:
            url = f"{self.base_url}&start={start}"
            print(f"Navigating to page starting at {start}...")

            max_retries = 3
            for attempt in range(max_retries):
                try:
                    self.driver.get(url)
                    break  # success
                except TimeoutException as e:
                    print(f"[Attempt {attempt + 1}] Timeout while accessing {url}")
                    if attempt == max_retries - 1:
                        print("Saving screenshot for debugging...")
                        self.driver.save_screenshot("timeout_debug.png")
                        raise e
                    time.sleep(5)

            try:
                self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".srpResultCardContainer")))
            except TimeoutException:
                print(f"No job cards found on page starting at {start}. Ending scraping.")
                break

            print("🔍 Scraping job data...")
            cards = self.driver.find_elements(By.CSS_SELECTOR, ".srpResultCardContainer")
            jobs = []

            for card in cards:
                title = self._safe_get_text(card, ".jobTitle")
                company = self._safe_get_text(card, ".companyName p")
                location = self._safe_get_text(card, ".cardBody .details.location")

                experience = ''
                salary = ''
                try:
                    rows = card.find_elements(By.CSS_SELECTOR, ".experienceSalary .bodyRow")
                    for row in rows:
                        text = self._safe_get_text(row, ".details")
                        if "year" in text.lower():
                            experience = text
                        elif "$" in text or "SGD" in text:
                            salary = text
                except NoSuchElementException:
                    pass

                if title:
                    jobs.append({
                        "title": title,
                        "company": company,
                        "location": location,
                        "experience": experience,
                        "salary": salary
                    })

            print(f"Page {start // self.limit + 1}: Scraped {len(jobs)} job(s).")

            if not jobs:
                print(" No jobs found. Ending scraping.")
                break

            all_jobs.extend(jobs)
            start += self.limit
            time.sleep(1)

        print(f"Scraped total {len(all_jobs)} jobs.")
        self.driver.quit()

        print(f" Saving total {len(all_jobs)} jobs to all-jobs.json...")
        with open("foundit-jobs.json", "w", encoding="utf-8") as f:
            json.dump(all_jobs, f, indent=2, ensure_ascii=False)

        return pd.DataFrame(all_jobs)
