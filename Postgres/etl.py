import os
import glob
import psycopg2
import pandas as pd
import os
from sql_queries import *

# Storing current directory
outputFilepath=os.getcwd()

def process_song_file(cur, filepath):
    """
    Description: This function can be used to read the file in the filepath (data/song_data)
    to get the song and artist info and used to populate the songs and artists dim tables.

    Arguments:
        cur: the cursor object. 
        filepath: log data file path. 

    Returns:
        None
    """
    # open song file
    df = pd.read_json(filepath,lines=True)
    # insert song record
    # save Dataframe to csv file and copy records to csv
    df[['song_id','title','artist_id','year','duration']].to_csv(
        outputFilepath+'/song.csv',index=False,header=True)
    copyfromcsv(cur,'song')   
    
    # insert artist record
    # save Dataframe to csv file and copy records
    df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].to_csv(
        outputFilepath+'/artist.csv',index=False)
    copyfromcsv(cur,'artist')
    cur.execute(update_songs)
    
    
def process_log_file(cur, filepath):
    """
    Description: This function can be used to read the file in the filepath (data/log_data)
    to get the user and time info and used to populate the users and time dim tables.

    Arguments:
        cur: the cursor object. 
        filepath: log data file path. 

    Returns:
        None
    """
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[(df.page == 'NextSong') & 
            (df.ts is not None) & 
            (df.userId is not None) &
            (df.sessionId is not None)] 
    
    # sort by timestamp
    df.sort_values(by='ts',inplace=True)
    
    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'],unit='ms')
    
    # insert time data records
    # save Dataframe to csv file and copy records
    pd.DataFrame({'time_stamp':t,
                  'hour':t.dt.hour,
                  'day':t.dt.day,
                  'week':t.dt.week,
                  'month':t.dt.month,
                  'year':t.dt.year,
                  'weekday':t.dt.weekday}
                ).to_csv(outputFilepath+'/time.csv',index=False)
    copyfromcsv(cur,'time')
    
    # load user table
    df[['userId','firstName','lastName','gender','level']].drop_duplicates(
        subset='userId', keep='last').to_csv(
        outputFilepath+'/user.csv',index=False)
    copyfromcsv(cur,'user')    
    
    # insert songplay records creating temporary list to contain all records
    lsTemp=[]
    for _, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        lsTemp.append(pd.DataFrame({'start_time':[pd.to_datetime(row['ts'],unit='ms')],
                                    'user_id':[row.userId],
                                    'level':[row.level],
                                    'song_id':[songid],
                                    'artist_id':[artistid],
                                    'session_id':[row.sessionId],
                                    'location':[row.location],
                                    'user_agent':[row.userAgent]}))
    pd.concat(lsTemp).to_csv(outputFilepath+'/songplay.csv',index=False)
    copyfromcsv(cur,'songplay')    


def process_data(cur, conn, filepath, func):
    """
    Description: This function can be used to read all the file in the filepath (data/log_data or data/song_data)
    to process each file.
    Arguments:
        cur: the cursor object.
        con : the connection object
        filepath: log/song data folder path. 
        func: function to operate if 
            song folder process_song_file
            log folder process_log_file
    Returns:
        None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        print('{}/{} files processed.'.format(i, num_files))


def copyfromcsv(cur,table):
    """
    Description: This function can be used to read all the processed csvs for each table.
        -First all records from csv will be processed removing duplicates
        -And then pushed into a temprory stagging table and then pushed to main table
    Arguments:
        cur: the cursor object.
        table: name of the table in which the data has to be added 
    Returns:
        None
    """
    df=pd.read_csv(outputFilepath+'/'+table+'.csv')
    df=df.drop_duplicates()
    df.to_csv(outputFilepath+'/'+table+'.csv',index=False)
    cur.execute(copy_drop)
    cur.execute(eval('stagging_'+table))
    cur.execute(eval(table+'_stagging_copy')%(outputFilepath+'/'+table+'.csv'))
    cur.execute(eval(table+'_table_insert'))
    os.remove(outputFilepath+'/'+table+'.csv')

    
def main():
    """
    Description: This function can be used to connect to database and process song and log data files
    and push records into facts and dim tables
    Arguments:
        None
    Returns:
        None
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    conn.autocommit=True
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    conn.close()


if __name__ == "__main__":
    main()