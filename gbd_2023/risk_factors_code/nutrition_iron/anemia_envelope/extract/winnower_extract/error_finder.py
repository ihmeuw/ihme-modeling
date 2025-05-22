import os

def parse_error_file(file_path):
	error_file = open(file_path,'r') #open error file with read privileges
	bad_id_vec = [] #the vector to be returned
	curr_ubcov_id = 0 #initialize ubcov id place holder
	for line in error_file: #for each line in the output error file
		dis_line = line.strip()
		if "Running extraction for" in dis_line:
			dis_line = line.split()
			for word in dis_line:
				curr_ubcov_id = word 
		elif "Error" in dis_line or "Killed" in dis_line or "killed" in dis_line:
			bad_id_vec.append(int(curr_ubcov_id))

	return bad_id_vec

