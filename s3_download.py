#!/usr/bin/env python
glob_log = ["---START OF FILE---"]

def writing_log():
	with open("log_file.txt", "w") as log:
		for entry in glob_log:
			log.write(entry + "\n")
		log.write("---END OF FILE---")

def download_file(s3_session, current_bucket, current_file):
	global glob_log
	BUCKET_NAME = current_bucket
	FILE_NAME = current_file
	glob_log.append("> "+BUCKET_NAME+" ==> "+FILE_NAME)
	try:
		response = s3_session.download_file(BUCKET_NAME, FILE_NAME, "DOWNLOADS3/"+FILE_NAME)
		glob_log.append("	Download successful")
	except:
		glob_log.append("	Download FAILED")

def checkforfolder():
	newpath = "./DOWNLOADS3"
	if not os.path.exists(newpath):
		os.makedirs(newpath)

def file_decoder(filestring):
	main_dict = {}
	entrylist = filestring.split(";")
	for entry in entrylist:
		try:
			bucket_files = entry.split(":")
			current_bucket = bucket_files[0]
			current_filestr = bucket_files[1]
			files_list = current_filestr.split(",")
			temp_list = []
			for f in files_list:
				temp_list.append(f)
			main_dict[current_bucket] = temp_list
		except:
			pass
	return(main_dict)

def create_session(session, s3_key, s3_secret, s3_endpoint):
	global glob_log
	glob_log.append("Try building s3 client:")
	try:
		s3_client = session.resource(
			config=Config(signature_version='s3v4'),
			aws_access_key_id=s3_key,
			aws_secret_access_key=s3_secret,
			endpoint_url=s3_endpoint
			)
		glob_log.append("> Creating was successful")
		return(s3_client)
	except:
		glob_log.append("> Creating FAILED")

def return_signature(args):
	protocol = ''
	if args.s3signature == None or args.s3signature == '':
		protocol = 's3'
	else:
		protocol = args.s3signature

	return(protocol)

def argparsefunc():
	parser = argparse.ArgumentParser()
	parser.add_argument("s3files", type=str, help="String structure is 'bucketname1:file1,file2;bucketname2:file3'")
	parser.add_argument("s3key", type=str, help="the s3 object storage key")
	parser.add_argument("s3secret", type=str, help="the corresponding s3 secret key")
	parser.add_argument("s3endpoint", type=str, help="the connection endpoint of the specific s3")
	parser.add_argument("--s3signature", type=str, help="the protocol type of the s3 storage. Default: 's3'")
	args = parser.parse_args()
	return args
	
def main():
	global glob_log
	args = argparsefunc()
	s3_files = args.s3files
	s3_key = args.s3key
	s3_secret = args.s3secret
	s3_endpoint = args.s3endpoint
	s3_signature = return_signature(args)
	
	glob_log.append("Demanded files:"); glob_log.append("> "+s3_files)
	
	session = boto3.client(
		's3',
		config=Config(signature_version=s3_signature),
		aws_access_key_id=s3_key,
		aws_secret_access_key=s3_secret,
		endpoint_url=s3_endpoint
		)
		
	file_dict = file_decoder(s3_files)
	glob_log.append("Decoded file input:"); glob_log.append("> "+str(file_dict))
	
	checkforfolder()
	glob_log.append("Downloading files:")
	for bucket in file_dict:
		current_filelist = file_dict[bucket]
		for current_file in current_filelist:
			download_file(session, bucket, current_file)
		
	writing_log()

if __name__ == "__main__":
	from botocore.client import Config
	import argparse
	import boto3
	import re
	import os
	main()