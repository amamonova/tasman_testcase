<!-- ABOUT THE PROJECT -->
## About The Project

An end-to-end task including connecting to [usajobs.gov](https://www.usajobs.gov), 
making sense of its data, and reporting on it.
The script creates and populates a local SQLite database and sends reports on a daily basis to an email address.

## Details

With the first running a database is created; default name is `tasman_db`. 
The script makes API call to usajobs, extract necessary info and insert it into a table with name
`position` as default. In order to prevent downloading and storing duplicates, we calculate 
the most recent publication date from the DB and extract data only for necessary dates.

Then, the result of 3 SQL queries are stored in report path and also attached to email message.
Sending to email is developed with local developer SMTP server.

<!-- GETTING STARTED -->
## Getting Started

### Installation

1. Get a free API Key at [usajobs.gov](https://developer.usajobs.gov/APIRequest/Index)
2. Clone the repo
   ```sh
   git clone https://github.com/amamonova/tasman_testcase.git
   ```
3. Set environment variable AUTHORIZATION_KEY with API token  
   ```sh
    export AUTHORIZATION_KEY='your_api_token'
   ```
4. Install requirements 
   ```sh
   pip install -r requirements.txt
   ```
5. For sending reports test SMTP server should be up
   ```sh
   sudo python -m smtpd -c DebuggingServer -n localhost:1000
   ```


<!-- USAGE EXAMPLES -->
## Usage

The script can be specified by values: 
1. postions_title - job titles for search compilation
2. keywords - keywords for search compilation
3. recipient_email - email to send reports

For searching jobs with keywords 'Data' or 'Analysis'
   ```sh
   python main.py --keywords 'Data' 'Analysis'
   ```
If variables are not specified, the searching will be with default values: 
1. position_title: `'Data Analyst', 'Data Scientist', 'Data Engineer'`
2. keywords: `'data', 'analysis', 'analytics'`
3. recipient_email: `test@example.com`

For scheduled running script (every day at 5AM):
   ```sh
   crontab -e
   ```
Add a row with schedule:
   ```sh
   0 5 * * * python main.py 
   ```
Note, if you use virtualenv you should specify the full path to Python. 
Also, don't forget to write full path for the main script, as well.  