
# local/custom libraries
from slacker import Slacker
import submitter

# standard libraries
import sys

# remote/virtual environment libraries
from elmo.run import upload_epi_data, validate_input_sheet


def upload_dataset(bundle_id, out_dir, status_dir):
	"""
	Upload the data to the Epi-database for the given bundle ID
	"""

	# excel file to upload
	excel_file = "{out_dir}new_inputs_{bundle_id}.xlsx".format(\
	                                                        out_dir=out_dir,
														    bundle_id=bundle_id)
	print bundle_id
	print excel_file
	
	# upload
	status = upload_epi_data(bundle_id,
							 excel_file)
	
	print status
	
	message = ("Inputs for {bundle_id} has {result} for the upload to the "
	           "Database").format(bundle_id=bundle_id,
							      result=str(status.loc[0, 'request_status']))
							 
	return message


def validate_dataset(bundle_id, out_dir, status_dir):
	"""
	validates the data for upload to the Epi-database for the given bundle ID.
	"""

	# excel file to upload
	excel_file = "{out_dir}new_inputs_{bundle_id}.xlsx".format(\
	                                                        out_dir=out_dir,
															bundle_id=bundle_id)
	# validate														   
	status = validate_input_sheet(bundle_id,
								  excel_file,
								  status_dir)
								  
	print status
	
	# get the result of validation
	message = ("Inputs for {bundle_id} have {result} validated for upload to "
	           "the Database").format(bundle_id=bundle_id,
								      result=str(status.loc[0, 'status']))
	
	
	return message

if __name__ == '__main__':
	"""MAIN"""
	
	# bring in args
	bundle_id, \
	upload, \
	out_dir, \
	status_dir, \
	send_Slack, \
	channel, \
	token = sys.argv[1:8]
	
	if send_Slack == "YES":
		print "send_Slack: ", send_Slack
		token = str(token)
		slack = Slacker(token)
		channel = str(channel)
	else:
		slack = None
		channel = None
	
	if upload == "UPLOAD":
		message = upload_dataset(bundle_id, out_dir, status_dir)
	else:
		message = validate_dataset(bundle_id, out_dir, status_dir)
	
	if send_Slack == "YES":
		slack.chat.post_message(channel, message)
									  
		


