'''	 Author: NAME
	 Date:   11/18/16
	 Purpose: Scrapes data from 2016 wikipedia terorrism pages.
	 Problem:  Data are tabluated by conflict and year, but selecting multiple conflicts only allows the user
				to select overlapping years. Furthermore, data are returned as HTML tables;
				they are just printed on the page. We want to get deaths due to each conflict,
				for all years data exists, in one table. 
	 Logic:   Starting with the type of table we want already selected (with the base url),
				use the selenium package's webdriver to automate our browsing. Using an old version of
				Firefox, open a browser, select each conflict's checkbox iteratively, click through to the next page,
				then select all year checkboxes for that conflict. Buttons and checkboxes are selected by xpath;
				this is a way of specifying the text contained within the HTML tags of items we want. 
				These tags must be found by right-clicking the page of interest and selecting 'inspect element'
				or similar, and discovering the elements of the page you desire. 
				Next, click through to the page displaying the table,
				and grab it using pandas' read_html method. Format the data for cleaner output, append them
				to a continuously growing table, and then go back and to the conflict page and repeat.
				Clean up any issues with non-ascii values in the table and save it to file.
	 Dependencies:
			Requries selenium and pandas (use "easy_install _______" or "pip install ______" replacing the
				underscores with the desired package name.
			Requires Firefox v.25.0... I tried the most recent version (49?), but it didn't work. Downgrading to v25
				seemed to solve the problem.'''

''' TODO: 	Let the script take the year(s) as an argument... use argparse!'''

from selenium import webdriver
import pandas as pd
import time   # for time.sleep
import string # for string.printable

# variables
base_url = 'https://en.wikipedia.org/wiki/List_of_terrorist_incidents,_' 
out_path = r'FILEPATH'

years = ['2016'] 
months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

def go_to_next_page(month):
	x_path_str = "//*[contains(@href, " + '\'' + month + '\'' + ")]"
	next_button = browser.find_element_by_xpath(x_path_str)
	next_button.click()

def get_ref_url(): # <-- 11/21/16 resume here to scrape sources for datapoints!

# converts a specific
def ascii_convert(s):
    try:
        return filter(lambda x: x in string.printable, s)
    except:
        return s

df_list = [] # empty list to store dataframes in as we grab them
df_list_idx = 0
# get conflicts page
browser = webdriver.Firefox() # note: Firefox v25 works, newer versions may have issues

# iterate over the chosen years
for year in years:
	browser.get(base_url + year)
	# iterate over months in year(s) chosen, grabbing data for all years available
	for month in months:
		time.sleep(1.5)				# without this the request frequency is too high (I think)
		try:
			go_to_next_page(month)
		except:
			print('No data for ' + month + ' ' + year)
			break

		# get data for all years of chosen conflict and format it nicely
		new_data = pd.read_html(browser.current_url)[0]
		# replace columns with first row
		new_data.columns = new_data.iloc[0]
		new_data.reindex(new_data.index.drop(0))
		new_data.drop(0, axis=0, inplace=True)
		new_data['month'] = month
		# regex wizardry to pre-clean death numbers
		# new_data.Dead.str.extract('(?P<best>([0-9]+ (?=\() ) ) (?P<low> (?<=\(\+) [0-9]+)')
		new_data['Dead'] = new_data['Dead'].str.extract('^\d+').astype(int)
 
		df_list.append(new_data)
		
		print('done with ' + month + ' ' + year + ' events')

		# go back to start page
		browser.back()

big_data = pd.concat(df_list)

# # Save table to file
big_data.to_csv(out_path, encoding='utf-8')

print("done with 2016 terrorism webscrape!")
browser.close()
