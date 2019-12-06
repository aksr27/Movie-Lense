import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="movielense"
)

mycursor = mydb.cursor()

try:
	mycursor.execute("CREATE DATABASE movielense")#creating database
	#creating tables in movielense database
	mycursor.execute("CREATE TABLE movie (item_id VARCHAR(100),title VARCHAR(1000))")
	mycursor.execute("CREATE TABLE data (user_id VARCHAR(100),item_id VARCHAR(100),rating VARCHAR(100),time_stamp VARCHAR(100))")
	mycursor.execute("CREATE TABLE user (user_id VARCHAR(100),age VARCHAR(100),gender VARCHAR(100),occupation VARCHAR(100),zip_code VARCHAR(100))")
	
except Exception as e:
	print(e)

#Inserting u.data file data values into table 'data'
fd='ml-100k/ml-100k/u.data'
with open(fd,'rt') as data:
	for line in data:
		x=line.split('\t')
		x[3]=x[3].rstrip('\n')
		print(x)
		query="INSERT INTO data VALUES(%s,%s,%s,%s)"
		mycursor.execute(query,(x[0],x[1],x[2],x[3]))
		mydb.commit()

#Inserting u.user file data values into table 'user'
fu='ml-100k/ml-100k/u.user'
with open(fu,'rt') as user:
	for line in user:
		x=line.split('|')
		x[4]=x[4].rstrip('\n')
		print(x[0])
		query="INSERT INTO user VALUES(%s,%s,%s,%s,%s)"
		mycursor.execute(query,(x[0],x[1],x[2],x[3],x[4]))
		mydb.commit()

#Inserting u.item file data values into table 'movie' only 2 columns are inserted
fm='ml-100k/ml-100k/u.item'
with open(fm,'rt') as movie:
	for line in movie:
		x=line.split('|')
		print(x[0])
		query="INSERT INTO movie VALUES(%s,%s)"
		mycursor.execute(query,(x[0],x[1]))
		mydb.commit()

#___________________________Queries_________________________________________

query1="SELECT COUNT(occupation),user_id FROM user WHERE occupation='artist'"
mycursor.execute(query1)
result = mycursor.fetchall()
print('Query-1\nArtist Count :',result[0][0])
print('\n')

query2="SELECT COUNT(occupation) FROM user WHERE occupation='artist' AND age >=25"
mycursor.execute(query2)
result = mycursor.fetchall()
print('Query-2\nArtist Count < 25 age :',result[0][0])
print('\n')

query3="SELECT AVG(rating) FROM data"
mycursor.execute(query3)
result = mycursor.fetchall()
print('Query-3\nAverage Movie Rating :',result[0][0])
print('\n')

query4="SELECT movie.title,AVG(data.rating) FROM data INNER JOIN movie ON data.item_id=movie.item_id WHERE data.item_id=1"
mycursor.execute(query4)
result = mycursor.fetchall()
print('Query-4\nName of movie :',result[0][0],'\nAverage Rating :',result[0][1])
print('\n')

query5="SELECT movie.title,AVG(data.rating) FROM data INNER JOIN movie ON data.item_id=movie.item_id WHERE data.item_id=2"
mycursor.execute(query5)
result = mycursor.fetchall()
print('Query-5\nName of movie :',result[0][0],'\nAverage Rating :',result[0][1])
print('\n')

query6="SELECT gender,zip_code FROM user  WHERE user_id=(SELECT MAX(user_id) FROM data)"
mycursor.execute(query6)
result = mycursor.fetchall()
print('Query-6\nGender:',result[0][0],'\nZip Code :',result[0][1])
print('\n')


"""
4.
One of the most interesting observation of movie lens dataset is we can observe high increase
of the sci-fi as well as animated movies over last 5 decades, which can suggest the taste of present generation so
that director will try to make movies what viewers want.

One of the most interesting thing which comes from the analysis of movie making is that people
like to see the movies which best reflect the present scenarios,like during wartime people
most liked the wartime based movies like during WW2 ,Afganistan war,Iraq war,Vietnam war etc.
So the best thing is to make a movie which will very close to the imagination of the present
generation and may be possible in future or currently ongoing and affecting wide range of
population.This will be great! 

"""