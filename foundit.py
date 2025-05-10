from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
import time

# Setup Chrome options
options = Options()
options.add_argument("--start-maximized")  # Open browser in maximized mode
# options.add_argument("--headless")  # Uncomment to run in headless mode

# Start the driver
print("🚀 Launching browser...")
driver = webdriver.Chrome(service=ChromeService(), options=options)
wait = WebDriverWait(driver, 15)

base_url = "https://www.foundit.sg/srp/results?sort=1&limit=15&query=%22%22&quickApplyJobs=true&industries=information+technology%2Csoftware%2Csoftware+engineering%2Cit+management"
start = 0
limit = 15
all_jobs = []

while True:
    url = f"{base_url}&start={start}"
    print(f"🌐 Navigating to page starting at {start}...")
    driver.get(url)

    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".srpResultCardContainer")))
    except TimeoutException:
        print(f"❌ No job cards found on page starting at {start}. Ending scraping.")
        break

    print("🔍 Scraping job data...")
    cards = driver.find_elements(By.CSS_SELECTOR, ".srpResultCardContainer")
    jobs = []

    for card in cards:
        try:
            title = card.find_element(By.CSS_SELECTOR, ".jobTitle").text.strip()
        except NoSuchElementException:
            title = ''

        try:
            company = card.find_element(By.CSS_SELECTOR, ".companyName p").text.strip()
        except NoSuchElementException:
            company = ''

        try:
            location = card.find_element(By.CSS_SELECTOR, ".cardBody .details.location").text.strip()
        except NoSuchElementException:
            location = ''

        experience = ''
        salary = ''

        try:
            rows = card.find_elements(By.CSS_SELECTOR, ".experienceSalary .bodyRow")
            for row in rows:
                try:
                    text = row.find_element(By.CSS_SELECTOR, ".details").text.strip()
                    if "year" in text.lower():
                        experience = text
                    elif "$" in text or "SGD" in text:
                        salary = text
                except NoSuchElementException:
                    continue
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

    print(f"✅ Page {start // limit + 1}: Scraped {len(jobs)} job(s).")

    if not jobs:
        print("🚫 No jobs found. Ending scraping.")
        break

    all_jobs.extend(jobs)
    start += limit
    time.sleep(1)  # Avoid hammering the server too fast

# Save to JSON
print(f"💾 Saving total {len(all_jobs)} jobs to all-jobs.json...")
with open("foundit-jobs.json", "w", encoding="utf-8") as f:
    json.dump(all_jobs, f, indent=2, ensure_ascii=False)

print("✅ Done. Output saved in foundit-jobs.json")
driver.quit()
