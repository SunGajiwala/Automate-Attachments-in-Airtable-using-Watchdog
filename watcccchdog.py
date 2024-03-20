import sys
import os
import re
import time
import logging
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime
from airtablee import get_airtable_data
from airtablee import send_urlattachment_to_airtable
from aws_s3 import upload_files_to_s3
from aws_s3 import delete_files_in_folder

# Define a custom event handler
class MyHandler(FileSystemEventHandler):
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def extract_trans_no(self, filename):
        # Extract the transaction number from the filename
        trans_no = filename.split('_')[0]
        return trans_no

    def on_created(self, event):
        if event.is_directory:
            return
        filepath = os.path.abspath(event.src_path)  # Convert relative path to absolute path
        filename = os.path.basename(event.src_path)  # Extracting file name from path
        trans_no = self.extract_trans_no(filename)
        url = f'https://d35ajrr6zfgx6v.cloudfront.net/docs/{filename}'
        if re.match(r'^[a-zA-Z0-9].*_([0-9]|[0-9][0-9])_.*(\.pdf|\.msg)$', filename):  # Match a string of alphabets followed by a number and underscore
            self.dataframe = self.dataframe._append({'Path': filepath, 'Filename': filename, 'Trans#': trans_no, 'public_Url': url}, ignore_index=True)
            print("File added:", filename)
            print(self.dataframe)

    def run_at_specific_time(self):
        airtable_data = get_airtable_data()
        filtered_df = self.dataframe[self.dataframe['Trans#'].isin(airtable_data['Trans#'])]
        # print("Filtered DataFrame:")
        # print(filtered_df.shape)
        bucket_name = 'kptest2'
        upload_files_to_s3(filtered_df,bucket_name)
        time.sleep(10)
        send_urlattachment_to_airtable(filtered_df)
        time.sleep(10)
        folder_name = 'docs/'  # Note the trailing slash to specify the folder
        delete_files_in_folder(bucket_name, folder_name)

if __name__ == "__main__":
    # Set the format for logging info
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    # Set format for displaying path
    path = sys.argv[1] if len(sys.argv) > 1 else '.'

    # Create an empty DataFrame to store file information
    df = pd.DataFrame(columns=['Path', 'Filename', 'Trans#', 'public_Url'])

    # Initialize custom event handler
    event_handler = MyHandler(df)

    # Initialize Observer
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)

    # Start the observer
    observer.start()
    try:
        while True:
            now = datetime.now()
            if (now.hour == 8 and now.minute == 30):
                event_handler.run_at_specific_time()
                time.sleep(70)
            else:
                time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()