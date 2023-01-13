import mechanicalsoup
from datetime import datetime, timedelta
import time
import keyring
import getpass
import pandas as pd
import zipfile
import io

class RITIS_Downloader:
    '''
    Download INRIX XD segment data using the RITIS Massive Data Downloader.

    Will include all XD segments from the text file segments.txt, which must be populated by the user before hand.
    The user can specify a date range to be used for a single download, 
    or for automated daily downloads run the update() function which submits jobs one day at a time until yesterday, starting with the date saved in the last_run.txt file.
    
    '''
    def __init__(self, segments_path='XD_segments.txt', download_path='Data/', start_time='06:00:00', end_time='20:00:00', 
        bin_size=15, units="seconds", columns = ["speed","average_speed","reference_speed","travel_time_minutes","confidence_score","cvalue"],
        confidence_score=[30]):
        
        # Set user variables
        self.download_path = download_path
        self.start_time = start_time
        self.end_time = end_time
        self.bin_size = bin_size
        self.units = units
        self.columns = columns
        self.confidence_score = confidence_score

        # Get XD segments list
        with open(segments_path, 'r') as file:
            self.xd_segments = file.read().split(',')

        # Set URLs
        self.url = 'https://pda.ritis.org/suite/download/' #page to log in to
        self.url_submit = 'https://pda.ritis.org/export/submit/' #link of the submit button to submit jobs to
        self.url_history = 'https://pda.ritis.org/api/user_history/' #API that returns job history

    # Link that returns the folder contents for a job
    def download_link(self, uuid):
        return f'https://pda.ritis.org/export/download/{uuid}?dl=1'

    def get_credentials(self):
        try:
            email = keyring.get_password('RITIS', 'email')
            password = keyring.get_password('RITIS', email)
            assert(email != None)
            assert(password != None)
            print('Credentials retreived')
        except Exception:
            email = input('\n\nEnter email for RITIS account:\n')
            password = getpass.getpass('Enter Password: ')
            save = input("\nStore email/password locally for later? Manage them later using the Windows Credential Manager.\nStore email/password in Credential Manger? Type YES or NO: ")
            if save.lower() == 'yes':
                keyring.set_password('RITIS','email', email)
                keyring.set_password('RITIS', email, password)
                print('\n\Email and password saved in Credential Manager under RITIS.')
                print('There are two credentials with that name, one used to look up email, the other uses email to look up password.')
        return email, password 

    def login(self):
        email, password = self.get_credentials()
        browser = mechanicalsoup.StatefulBrowser()
        browser.open(self.url, verify=False)
        browser.select_form()
        browser['username'] = email
        browser['password'] = password
        response = browser.submit_selected()
        print(response)
        return browser, email


    def submit_job(self, browser, email, start_date, end_date, name):
        
        # Set datetime range
        start = f'{start_date} {self.start_time}'
        end = f'{end_date} {self.end_time}'

        # Plug variables into data json. This was derived from the POST that gets sent by clicking the SUBMIT button.
        # Using the Dev Tools network tab, the cURL was coppied and transformed into json by ChatGPT. Thank you, AI overlord!
        # This idea was inspired by https://www.youtube.com/watch?v=DqtlR0y0suo
        data = {
            "DATASOURCES": [{
                "id": "inrix_xd",
                "columns": self.columns,
                "quality_filter": {
                    "thresholds": self.confidence_score
                }
            }],
            "ROADPROVIDER": "inrix_xd",
            "TMCS": self.xd_segments,
            "ROAD_DETAILS": [{
                "SEGMENT_IDS": self.xd_segments,
                "DATASOURCE_ID": "inrix_xd",
                "ATLAS_VERSION_ID": 49
            }],
            "DATERANGES": [{
                "start_date": start,
                "end_date": end
            }],
            "ENTIREROAD": False,
            "NAME": name,
            "DESCRIPTION": "Why did the traffic signal cross the road?",
            "AVERAGINGWINDOWSIZE": self.bin_size,
            "EMAILADDRESS": email,
            "SENDNOTIFICATIONEMAIL": False,
            "ADDNULLRECORDS": False,
            "TRAVELTIMEUNITS": self.units,
            "COUNTRYCODE": "USA"
        }

        response = browser.post(self.url_submit, json=data)
        print(response)


    def get_dates(self):
        # Returns a list of dates that need to be updated through yesterday
        
        yesterday = datetime.now() - timedelta(days=1)
        date_list = []
        # Get the date of the last run
        with open('last_run.txt', 'r') as f:
            last_run = datetime.strptime(f.read(), '%Y-%m-%d')

        while last_run <= yesterday:
            date_list.append(last_run.strftime("%Y-%m-%d"))
            last_run += timedelta(days=1)
        
        return date_list

    def update_job_status(self, browser, jobs):
        # Job status from RITIS, in JSON format
        history = browser.post(self.url_history).json() 
        # Update each job with uuid and status (pending=1, ready=2, downloaded=3)
        for key, value in jobs.items():
            for data in history:
                if data["description"] == key:
                    jobs[key]['uuid'] = data["uuid"]
                    jobs[key]['status'] = data["status"]
                    jobs[key]['downloaded'] = data["downloaded"]
                    break
        return jobs

    # Function by ChatGPT, extracts file from zipped folder without saving to local drive so less work overall
    def extract_file_to_df(self, data, file_name):
        # Create a BytesIO object from the data
        data = io.BytesIO(data)
        # Open the zip file from the BytesIO object
        with zipfile.ZipFile(data) as zip_ref:
            # Extract the specific file to a BytesIO object
            with zip_ref.open(file_name) as file:
                extracted_file = io.BytesIO(file.read())
        # Create a pandas dataframe from the extracted file
        df = pd.read_csv(extracted_file, parse_dates=['measurement_tstamp'])
        df = df.set_index(['xd_id', 'measurement_tstamp'])
        df.index.names = ['XD', 'TimeStamp']
        df = df.astype('float32')
        df = df.sort_index(level=0)
        return df

    def download_job(self, browser, jobs):
        # Download all jobs that are ready
        # This downloads the data, the content has to be saved as a zip folder
        for key, value in jobs.items():
            if jobs[key]['status'] == 3 and jobs[key]['downloaded'] == False:
                print(f'Downloading {key}')
                url = self.download_link(jobs[key]['uuid'])
                response = browser.open(url)
                # Extract file into dataframe
                df = self.extract_file_to_df(response.content, f'{key}.csv')
                df.to_parquet(f'{self.download_path}{key}.parquet')
                print('Saved parquet file for ', key)


    def download_all_remaining(self, browser, jobs):
        # Download all remaining jobs
        print(jobs)
        while any(job['status'] != 3 for job in jobs.values()):
            time.sleep(60)
            jobs = self.update_job_status(browser, jobs)
            print(jobs)
            self.download_job(browser, jobs)                     

    def run(self):

        # All the dates that need to be run, these will be iterated through
        date_list = self.get_dates()
        # Initiate dicitonary to track job status
        jobs = {key: {'status': 0, 'uuid': ""} for key in date_list}

        # Initiate browser and log in
        browser, email = self.login()

        # Step through each date one at a time, from the last run date until today
        for date in date_list:
            self.submit_job(browser, email, start_date=date, end_date=date, name=date)
            # Wait a little and then update status of each job, and download those that are ready
            time.sleep(120)
            jobs = self.update_job_status(browser, jobs)
            self.download_job(browser, jobs)

        # Download any remaining jobs
        self.download_all_remaining(browser, jobs)            

        # Close the browser
        browser.close()

        # Update the last run date
        with open('last_run.txt', 'w') as f:
            f.write(datetime.now().strftime('%Y-%m-%d'))

    def single_run(self, start_date, end_date, job_name):
        # Replace any spaces with underscore
        job_name = job_name.replace(' ', '_')
        # Initiate dicitonary to track job status
        jobs = {job_name: {'status': 0, 'uuid': ""}}
        # Initiate browser and log in
        browser, email = self.login()    
        # Submit job
        self.submit_job(browser, email, start_date=start_date, end_date=end_date, name=job_name)    
        # Download
        self.download_all_remaining(browser, jobs)
        # Close the browser
        browser.close()

