import psycopg2
import pandas as pd
import logging
import os
import math
import requests
import eyed3
import multiprocessing

RACKSPACE_API_USERNAME = 'ciscoa'
RACKSPACE_API_KEY = "AAABkOTrkjiAllbLvb96qbIfNj-46bK4ii2vitI85K6jNhf-3K9n4nh31803UCOwOeMLxmwY--tkwJ0jN4Q9LamjEqK5kH_VPz2EFqvOhRdRpPqtnAJzK6ihpv73gBrZE074dvGLEB1vzCx0Gya5SjSFTdP2dUIvUMA"
RACKSPACE_API_ENDPOINT = 'https://storage101.dfw1.clouddrive.com/v1/MossoCloudFS_17a7fad0-f8a9-4ba9-b368-3e662aa5cb99/'

def format_name_to_rackspace(postgres_file):
    """
    Formats filename from postgres field 'media file' to rackspace form  
    """
    
    try:
        file_split = postgres_file.split('/')
        if file_split[1] == 'message':
            folder = 'messaging_'+file_split[2]+'/'
        elif file_split[1] == 'song':
            folder = 'music_'+file_split[2]+'/'
        else:
            folder = 'production_'+file_split[2]+'/'
        file_uuid = file_split[-1]

        formatted_name = folder + file_uuid
    
    except:
        formatted_name = None

    return formatted_name

def setup_logger(logger_name):
    """
    Logs process into files in a folder 'LOG_ROOT'
    """
    
    logger = logging.getLogger(logger_name)
    
    format = logging.Formatter('%(asctime)s - %(name)s - [%(levelname)s] - %(message)s')
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(format)
    file_handler = logging.FileHandler(os.path.join(LOG_ROOT, logger_name + '.log'), mode='w')
    file_handler.setFormatter(format)
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    
    return logger

def download_file_from_rackspace(data):
    """
    This function downloads files in the 'data['rs_paths']' list 
    and saves them into the 'output_dir' directory following the
    same hierarchy of the files in RackSpace.
    """
    
    rs_paths = data['rs_paths']
    core_num = data['core_num']

    logger_name = f'{core_num}_grrid_media'
    logger = setup_logger(logger_name)

    # Set the Rackspace authentication headers
    headers = {'X-Auth-User': RACKSPACE_API_USERNAME, 
               'X-Auth-Key': RACKSPACE_API_KEY}

    # Authenticate with Rackspace
    response = requests.get(RACKSPACE_API_ENDPOINT, headers=headers)

    # Set the headers for downloading the files
    headers = {'X-Auth-Token': RACKSPACE_API_KEY}

    # Download each file
    for ii, media_name in enumerate(rs_paths):
        is_valid_log = ''
        
        # Set the URL for the file
        media_url = RACKSPACE_API_ENDPOINT + media_name
        media_output_path = os.path.join(MEDIA_ROOT, media_url.split(RACKSPACE_API_ENDPOINT)[1])
        media_output_folder, media_output_file = os.path.split(media_output_path)

        # Download the file
        response = requests.get(media_url, headers=headers)

        # Save the file to disk
        if not os.path.exists(media_output_path):
            
            if not os.path.isdir(media_output_folder):
                os.makedirs(media_output_folder)
            
            with open(media_output_path, 'wb') as f:
                f.write(response.content)
            
            # Check if file is a valid mp3 file
            info = eyed3.load(media_output_path) 
            if not info:
                is_valid_log = ' [[MP3 NOT VALID]]'
                    
            # Log info into log file
            logger.info(f'DOWNLOADED {str(media_output_path)}' + is_valid_log)
        
        if (ii+1) % 1000 == 0:
            logger.info(f'PROCESSED {str(ii+1)} MEDIA of {str(len(rs_paths))}')
            print(f'PROCESSED {str(ii+1)} MEDIA of {str(len(rs_paths))}')


if __name__ == '__main__':

    # Connect to PostgreSQL db to obtain media file paths
    conn = psycopg2.connect(
        host="localhost",
        database="retail_radio",
        user="postgres",
        password="reallybadpassword"
    )

    # Dump media file paths into a dataframe
    media_paths_query = "SELECT media_file FROM production_media"
    df_grrid = pd.read_sql(media_paths_query, conn)
    conn.close()

    df_grrid = df_grrid.drop_duplicates()
    df_grrid = df_grrid.dropna()
    df_grrid = df_grrid[~df_grrid['media_file'].eq('')]

    # Format names to RackSpace structure
    media_grrid = set(df_grrid['media_file'])
    media_list_grrid = [format_name_to_rackspace(grrid_name) for grrid_name in media_grrid]
    media_list_grrid = media_list_grrid

    # Create directories to store files and logs
    MEDIA_ROOT = 'media/' #change if necesary for global path
    if not os.path.isdir(MEDIA_ROOT):
        os.makedirs(MEDIA_ROOT)
    LOG_ROOT = 'logs/' #change if necesary for global path
    if not os.path.isdir(LOG_ROOT):
        os.makedirs(LOG_ROOT)

    # Multiprocessing set up
    NUM_CORES = multiprocessing.cpu_count()
    NUM_THREADS = NUM_CORES*2
    chunk_size = math.ceil(len(media_list_grrid)/NUM_THREADS)
    pending_chunks = [media_list_grrid[i:i + chunk_size] for i in range(0, len(media_list_grrid), chunk_size)]
    assert media_list_grrid == [item for sublist in pending_chunks for item in sublist]

    pool_args = [{'rs_paths': pending_chunks[j], 'core_num': j} for j in range(NUM_THREADS)]

    with multiprocessing.Pool(NUM_THREADS) as p:
        result = p.map(download_file_from_rackspace, pool_args)
