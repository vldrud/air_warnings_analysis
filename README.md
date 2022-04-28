# air_warnings_analysis
That repository provides my project in analysis of air warnings on teritory of Ukraine during the Invasion Russia to Ukraine.

# The project processes.
My project has 3 stages: grabbing, parsing and analysis.
1. Grabbing - method of collection information taken from internet and stored as a raw. 
2. Parsing - method of processing information with filtering, cleaning, discovering and marking necessary information.
3. Analysis - executes by using Jupyter Notebook and looks like report.

The source of information is Telegram channel with air warning messages (i.e. sirena_dp)

## Grabbing
Grabbing is executed by grabber.py. That program works on telethon library and has the folllowing processes.
1. Log in from config.ini into Telegram.
2. From lists of channels in CHANNELS_LIST creates the loop and grab information from defined telegram channel
3. Saving in json format into data_raw directory
The main code is taken from internet...

## Parsing
Parsing is executed by parser.py. That program works on pandas in major and has the following processes:
1. Reading the json from data_raw directry
2. Filtering the information
3. Processing with regional and datetime data
4. Cleaning the Nans
5. Processing with message information and finding the time_delta values
6. Saving into .csv format into data directory


## Analysis
Analysis is executed by Jupyter Notebook and stored in reports directory
#### The last date of updating is 28.04.2022
