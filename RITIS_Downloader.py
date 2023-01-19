import mechanicalsoup
#import certifi
from datetime import datetime, timedelta
import time
import keyring
import getpass
import pandas as pd
import zipfile
import io
import os

class RITIS_Downloader:
    '''
    Download INRIX XD segment data from the RITIS Massive Data Downloader. 
    
    Use at your own risk, this code is not guaranteed to work and is not officially supported by ODOT or RITIS.
    Please avoid placing excessive or redundant request, be considerate of the loads being placed on the server which impact other users.

    Three different methods of downloading data are provided, including:

        1. single_download() - One-time download for single date range
        2. daily_download() - Download data for each day starting at the date in the last_run file (default is last_run.txt) through yesterday. This method is intended to be called on a daily schedule, for example, use Windows task scheduler to run at 1am each morning.
        3. continuous_download() - Meant to run as a background process throughout the day. It downloads most recent data on regular intervals, like each hour. After the end_time has elapsed then the process terminates. This method is meant to be run on a daily schedule, just like the daily_download() method.
    
    Do not schedule both daily_download() and continuous_download() to run, only pick one method to use.
    When continuous_download() is run, it will call daily_download() first to make sure data is updated through yesterday, if needed.
    continuous_download() will update the last_run file each time new data is downloaded, so the process will pick up where it left off
    if interrupted. Please note continuous_download() can provide recent data but it is not real time. 

    Data will includes XD segments from user-specified text file (default=segments.txt)
    MechanicalSoup package is used to handle browser interaction/requests, it is built on top of the requests library. See documentation. 
        
    '''
    def __init__(self, segments_path='XD_segments.txt', download_path='Data/', start_time='00:00:00', end_time='23:59:00', 
        bin_size=15, units="seconds", columns = ["speed","average_speed","reference_speed","travel_time_minutes","confidence_score","cvalue"],
        confidence_score=[30, 20, 10], last_run='last_run.txt', continuous_download_interval=60):
        
        # Set user variables
        self.download_path = download_path #path data is downloaded to
        self.start_time = start_time #time of day range will be limited to between start_time and end_time
        self.end_time = end_time 
        self.bin_size = bin_size #bin size in minutes, choose from [5,10,15,60]
        self.units = units # 'seconds' or 'minutes'
        self.columns = columns #included columns, choose from ["speed","average_speed","reference_speed","travel_time_minutes","confidence_score","cvalue"]
        self.confidence_score = confidence_score #provide list including 10 and/or 20 and/or 30 [10,20,30] see RITIS help for details, but 30 is best
        self.last_run = last_run #file name storing date that daily_download() was last run
        self.continuous_download_interval = continuous_download_interval #interval at which continuous_download() will download new data, in minutes

        # Get XD segments list
        with open(segments_path, 'r') as file:
            self.xd_segments = file.read().split(',')

        # Set URLs
        self.url = 'https://pda.ritis.org/suite/download/' #page to log in to
        self.url_submit = 'https://pda.ritis.org/export/submit/' #link of the submit button to submit jobs to
        self.url_history = 'https://pda.ritis.org/api/user_history/' #API that returns job history

    # Link that returns the folder contents for a job
    def __download_link(self, uuid):
        return f'https://pda.ritis.org/export/download/{uuid}?dl=1'

    def __get_credentials(self):
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

    def __login(self):
        email, password = self.__get_credentials()
        browser = mechanicalsoup.StatefulBrowser()
        browser.open(self.url, verify=False)
        # Add certificaiton
        #browser.session.verify = certifi.where()
        #browser.session.trust_env = False
        #Try another thing
        #browser.open(self.url)
        browser.select_form()
        browser['username'] = email
        browser['password'] = password
        response = browser.submit_selected()
        print(response)
        return browser, email


    def __submit_job(self, browser, email, start_date, end_date, name, start_time=None, end_time=None):
        # Use default start/end times if none given
        if start_time is None:
            start_time = self.start_time
        if end_time is None:
            end_time = self.end_time
        
        # Create list of date ranges
        date_list = pd.date_range(start_date, end_date)
        date_list = [date.strftime("%Y-%m-%d") for date in date_list]
        date_ranges = [{'start_date': f'{date} {start_time}', 'end_date': f'{date} {end_time}'} for date in date_list]

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
            "DATERANGES": date_ranges,
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

        browser.post(self.url_submit, json=data)


    def __get_dates(self):
        # Returns a list of dates that need to be updated through yesterday
        
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        date_list = []
        # Get the date of the last run
        with open(self.last_run, 'r') as f:
            last_run = datetime.strptime(f.read(), '%Y-%m-%d %H:%M:%S').date()
   
        #while last_run <= yesterday:
        while last_run <= yesterday:
            date_list.append(last_run.strftime("%Y-%m-%d"))
            last_run += timedelta(days=1)
        
        return date_list

    def __update_job_status(self, browser, jobs):
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
    def __extract_file_to_df(self, data, file_name):
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

    def __download_job(self, browser, jobs):
        # Download all jobs that are ready
        # This downloads the data, the content has to be saved as a zip folder
        for key, value in jobs.items():
            if jobs[key]['status'] == 3 and jobs[key]['downloaded'] == False:
                print(f'Downloading {key}')
                url = self.__download_link(jobs[key]['uuid'])
                response = browser.open(url)
                # Extract file into dataframe
                df = self.__extract_file_to_df(response.content, f'{key}.csv')
                df.to_parquet(f'{self.download_path}{key}.parquet')
                print('Saved parquet file for ', key)


    def __download_all_remaining(self, browser, jobs, sleep=60):
        # Download all remaining jobs
        while any(job['status'] != 3 for job in jobs.values()):
            time.sleep(sleep)
            jobs = self.__update_job_status(browser, jobs)
            self.__download_job(browser, jobs)                     

    def daily_download(self):

        # All the dates that need to be run, these will be iterated through
        date_list = self.__get_dates()
        # If date_list is empty then exit function
        if not date_list:
            print("Data is already updated through yesterday.")
            return

        # Initiate dicitonary to track job status
        jobs = {key: {'status': 0, 'uuid': ""} for key in date_list}

        # Initiate browser and log in
        browser, email = self.__login()

        # Step through each date one at a time, from the last run date until today
        for date in date_list:
            self.__submit_job(browser, email, start_date=date, end_date=date, name=date)
            # Wait a little and then update status of each job, and download those that are ready
            time.sleep(120)
            jobs = self.__update_job_status(browser, jobs)
            self.__download_job(browser, jobs)

        # Download any remaining jobs
        self.__download_all_remaining(browser, jobs)            

        # Close the browser
        browser.close()

        # Update the last run date
        with open('last_run.txt', 'w') as f:
            f.write(datetime.now().strftime('%Y-%m-%d 00:00:00'))

    def single_download(self, start_date, end_date, job_name):
        # Replace any spaces with underscore
        job_name = job_name.replace(' ', '_').replace(':','')
        # Initiate dicitonary to track job status
        jobs = {job_name: {'status': 0, 'uuid': ""}}
        # Initiate browser and log in
        browser, email = self.__login()    
        # Submit job
        self.__submit_job(browser, email, start_date=start_date, end_date=end_date, name=job_name)    
        # Download
        self.__download_all_remaining(browser, jobs)
        # Close the browser
        browser.close()

    def continuous_download(self):
        if self.continuous_download_interval < self.bin_size:
            print(f'continuous_download_interval ({self.continuous_download_interval}) cannot be less than the bin_size ({self.bin_size}).')
            exit()

        # Call daily_download() first to update data through yesterday if needed
        self.daily_download()
    
        today = datetime.now().strftime("%Y-%m-%d")

        # Initiate browser and log in
        browser, email = self.__login()

        # Time that the while loop below will end after
        end_process_time = datetime.strptime(f'{today} {self.end_time}', "%Y-%m-%d %H:%M:%S")
        start_time = datetime.strptime(self.start_time, "%H:%M:%S").time()

        while datetime.now() <= end_process_time:
            # Get current datetime
            now = datetime.now()
            # Ok and now wait a bit longer to make sure all the data we want has come in before continuing with next job?
            time.sleep(15)
            # After downloading the job, this will determine how long to wait before submitting the next job
            current_minute = now.minute + now.hour * 60 # minute number of the day to track the update interval/schedule
            next_step = int(current_minute / self.continuous_download_interval) + 1 # next_step is the next time in the schedule an update will be called for.

            #read the last time data was updated
            with open(self.last_run, 'r') as f:
                last_run_datetime = datetime.strptime(f.read(), '%Y-%m-%d %H:%M:%S')
            name = last_run_datetime.strftime("%Y-%m-%d-%H%M") # it's important that only allowed characters are used.
            # Set start time
            start_time = max(start_time, last_run_datetime.time())
            start_time_str = start_time.strftime("%H:%M:%S")

            # Calculate the most recent end time to use, considering the bin size and current time
            end_time = now - timedelta(minutes=now.minute % self.bin_size, seconds=now.second, microseconds=now.microsecond)
            end_time_str = end_time.strftime("%H:%M:%S")
            
            # Initiate jobs dictionary for tracking
            jobs = {name: {'status': 0, 'uuid': ""}}
  
            # Submit job
            self.__submit_job(browser, email, start_date=today, end_date=today, start_time=start_time_str, end_time=end_time_str, name=name)    
            # Download
            self.__download_all_remaining(browser, jobs, sleep=30)

            # Combine files, if needed
            df = pd.read_parquet(f'{self.download_path}{name}.parquet') # Read in file that was just downloaded
            os.remove(f'{self.download_path}{name}.parquet') # And delete it
            #If today's file already exists, read and append it
            if os.path.isfile(f'{self.download_path}{today}.parquet'): 
                df2 = pd.read_parquet(f'{self.download_path}{today}.parquet')
                df = df.append(df2)
            # Save data as today's file
            df.to_parquet(f'{self.download_path}{today}.parquet')

            # Update last_run file with the end date/time of the last run
            with open(self.last_run, 'w') as file:
                file.write(f'{today} {end_time_str}')
            
            # Wait for specified interval before downloading again
            while (datetime.now().minute + datetime.now().hour * 60) / self.continuous_download_interval < next_step:
                time.sleep(5)

        # Close the browser
        browser.close()
        print("DONE")
            
             

                

            


