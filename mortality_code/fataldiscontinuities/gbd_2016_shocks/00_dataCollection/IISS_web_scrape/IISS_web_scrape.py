'''	 Author: NAME
	 Date:   10/7/16
	 Purpose: Scrapes data from IISS Armed Conflicts Database. Requires login credentials,
	 			see HUB page on Shocks/Sources Information. Once logged in, systematically 
				selects all years available for each conflict. Conflict-years missing
	 			data are left as NAs (NaNs), as no data is distinct from zero. 
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
	 Dependencies:  Requires login to ACD site
			Requries selenium and pandas (use "easy_install _______" or "pip install ______" replacing the
				underscores with the desired package name.
			Requires Firefox v.25.0
	
# TODO: use pd.concat instead of pd.append. Store dataFrames in a list, then use a list comprehension
#		to concatenate the data in one fell swoop. This should be faster and allows for
#		greater flexibility in dealing with NaNs.

from selenium import webdriver
import pandas as pd
import time   # for time.sleep
import string # for string.printable

base_url = 'https://acd.iiss.org/en/statistics/selectconflicts?r=92F8259DEB594076AC83C43C1D128A83' 
out_path = r'FILEPATH'

def go_to_next_page():
	next_button = browser.find_element_by_xpath("//*[contains(@id, 'BtnNext')]")
	next_button.click()

def select_all_years():
	year_boxes = browser.find_elements_by_xpath("//*[contains(@id, 'ChkYear')]")
	for box in year_boxes:
		box.click()

def select_all_conflicts():
	conflict_boxes = browser.find_elements_by_xpath("//*[contains(@id, 'ChkConflict')]")
	n_conflicts = len(conflict_boxes)
	return conflict_boxes, n_conflicts

# this function borrowed from emdat scraping code
def ascii_convert(s):
    try:
        return filter(lambda x: x in string.printable, s)
    except:
        return s

# get conflicts page
browser = webdriver.Firefox() # note: Firefox v25 works, newer versions may have issues
browser.get(base_url)
# get number of conflicts
cbs, n_conflicts = select_all_conflicts()

# iterate over conflicts, grabbing data for all years available
for i in xrange(n_conflicts):
	time.sleep(1.5)				# without this the request frequency is too high (I think)
	
	cboxes, n = select_all_conflicts() 	# need to reselect boxes, page is fresh each time
	box = cboxes[i]
	box.click()				# select conflict

	go_to_next_page()
	select_all_years()
	go_to_next_page()
	
	# get data for all years of chosen conflict and format it nicely
	new_data = pd.read_html(browser.current_url)[0]
	new_data.iloc[0,0] = "Conflict"
	new_data.columns = new_data.iloc[0]
	new_data = new_data.drop(0)
	new_data = new_data.drop(2)

	# because instantiating an empty dataFrame is weird; append data
	if i == 0:
		table = new_data.copy()
	else:
		table = table.append(new_data)
	
	print 'done with row ' + str(i) + ' of ' + str(n_conflicts)

	# go back to start page
	browser.back()
	browser.back()

# Resolve conflicts with non-ascii characters (adapted from hacking_emdat code)
t = table.reset_index(drop=True)
for c in t.columns:
    t[c] = t[c].map(lambda x: ascii_convert(x))
    print c

# Save table to file
t.to_csv(out_path + 'iiss_raw_data.csv')

print "done with IISS webscrape!"
browser.close()
