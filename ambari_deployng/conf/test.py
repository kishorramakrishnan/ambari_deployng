
def test(file_name,original_string,replacement_string):
	# Replace the file content
	with open(file_name, 'r+') as file:
		file_content = file.read()
		file.seek(0)
		file_content = file_content.replace(original_string, replacement_string)
		file.write(file_content)

test('/Users/kramakrishnan/test.txt',"REPLACED","REPLACED\n")