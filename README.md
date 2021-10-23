# Wide Receiver Reverse ETL
Uses @cwendt94 's python package [cwendt94/espn-api](https://github.com/cwendt94/espn-api) package to pull our league's data into Snowflake after some formatting and cleaning. I provide a step-by-step of how to get it to run, after cloning the repo.

# 0. Make sure you have Python installed
I ran this with Python3.8, I am not sure about earlier versions, but if step #1 works for you, you should be all set.

# 1. Make sure you have the necessary packages
In your shell, run:
```
pip install -r requirements.txt
```
to make prep your machine with all of the packages required.

# 2. Login to ESPN and follow the steps of this Wiki
Wiki for the api is located [here](https://github.com/cwendt94/espn-api/wiki).

You will need to get the `league_id`, `swid`, and `espn_s2` by following the steps [here](https://github.com/cwendt94/espn-api/discussions/150). This Loom [here](https://www.loom.com/share/1c8b642a55ba4c1cafc30562e0e9ed79) shows you how to get them. You will need them for the next part.

# 3. Input the credentials in the .env file
Make a `.env` file on your computer.
```
SF_ACCOUNT=    # Put the X from https:///[X].snowflakecomputing.com/ there, include the region!
SF_USER=       # Your Snowflake Credentials
SF_PASSWORD=   # Your Snowflake Credentials
SF_WH=         # Your Snowflake Warehouse
SF_DB=         # Your Snowflake Database
LEAGUE_ID=     # Input from Step 2
ESPN_S2=       # Input from Step 2
ESPN_SWID=     # Input from Step 2
CENSUS_SECRET= # Input from Step 5
CENSUS_SYNC_1= # Input from Step 5
CENSUS_SYNC_2= # Input from Step 5
CENSUS_SYNC_3= # Input from Step 5
```
Make sure all of the values are all the way to the left towards the `=` sign.

# 4. Run the script! 
Change the code as you would like. Our fantasy football league rules implement, during the regular season, 50% Head to Head wins, 50% Points For Wins (determined as the top 6 of 12 teams). 

Adjust these rules as you need for your league!

# 5. Sync the Data to Airtable.
Check out this [Loom](https://www.loom.com/share/213a0cffef614c7e91d23362a0bf92ca) if you want to see how I got my sync triggers from Census to put in Section 2 of the code.