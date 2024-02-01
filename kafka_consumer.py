#!/usr/bin/env python3

from kafka import KafkaConsumer
import mysql.connector
import json
import time


class Kafka__Consumer:
	def __init__(self,users_topic='users-topic',bands_topic='bands-topic',bootstrap_servers='localhost:9092',mysql_host='127.0.0.1',mysql_user='mysql1234',mysql_pass='mysql1234',mysql_port='33306',mysql_db='user'):
		self.users_topic=users_topic
		self.bands_topic=bands_topic
		self.bootstrap_servers=bootstrap_servers
		self.mysql_host=mysql_host
		self.mysql_user=mysql_user
		self.mysql_pass=mysql_pass
		self.mysql_port=mysql_port
		self.mysql_db=mysql_db

	def User_Consumer(self):
		user_consumer = KafkaConsumer(self.users_topic,bootstrap_servers=self.bootstrap_servers,auto_offset_reset='earliest',enable_auto_commit=True)
		user_records = user_consumer.poll(timeout_ms=2000)
		user_data = []
		for record in user_records.items():
			for data in record[1]: ##The 0 element has topic related data
				json_data = data.value.decode('utf-8')###returns binary##Getting the dict 
				user_data.append(json.loads(json_data))
		user_consumer.close()
		return user_data

	def Band_Consumer(self):
		band_consumer =  KafkaConsumer(self.bands_topic,bootstrap_servers=self.bootstrap_servers,auto_offset_reset='earliest',enable_auto_commit=True)
		band_records = band_consumer.poll(timeout_ms=2000)
		band_data = []
		for record in band_records.items():
			for data in record[1]: ##The 0 element has topic related data
				json_data = data.value.decode('utf-8')###returns binary##Getting the dict
				band_data.append(json.loads(json_data))
		band_consumer.close()
		return band_data

	def update_user(self):
		try:
			myconn = mysql.connector.connect(user=self.mysql_user,password=self.mysql_pass,port=self.mysql_port,database=self.mysql_db,host=self.mysql_host,autocommit=True)
			cur = myconn.cursor()
			cur.execute('SELECT user_id FROM music where is_complete is false')
			returned_data = cur.fetchall()
			current_users = []
			for rid in returned_data:
				current_users.append(rid[0])
			if len(current_users) == 0: ##For when all users are complete
				return
			user_data = self.User_Consumer()
			band_data = self.Band_Consumer()
			users_to_update = []
			sql = 'UPDATE music SET albums=%s,is_complete=%s where user_id=%s'
			for user in user_data:
				if user['user_id'] not in current_users:
					continue
				user_dict = {'user_id':user['user_id']}
				user_albums = []
				user_unique_bands = []
				user_band_counter = 0
				user_compl = 0 ##Fix for when bands are not published yet
				for band in band_data:
					if band['band_name'] in user['bands']:
						if band['band_name'] in user_unique_bands:
							continue
						else:
							user_unique_bands.append(band['band_name'])
						user_band_counter += 1
						for album in band['albums']:
							user_albums.append(album)
					user_compl = 1 if user_band_counter == 3 else 0
				if user_compl == 0: ###Updates a user only if its complete. If not all user's bands are not uploaded then it will not update the user on mysql 
					continue
				user_dict['albums'] = str(user_albums)
				user_dict['is_complete'] = user_compl
				users_to_update.append(user_dict)
			print(f'Users to update....{users_to_update}')
			for up_user in users_to_update:
				values = (up_user['albums'],up_user['is_complete'],up_user['user_id'])
				cur.execute(sql,values)
		except Exception as error:
					print(error)
		finally:
			if cur is not None:
				cur.close()
			if myconn is not None:
				myconn.close()

	def add_new_user(self):
		try:
			myconn = mysql.connector.connect(user=self.mysql_user,password=self.mysql_pass,port=self.mysql_port,database=self.mysql_db,autocommit=True)
			cur = myconn.cursor()
			cur.execute("SELECT user_id from music") ##Get current inserted users in sql
			returned_data = cur.fetchall()
			current_users = []
			for rid in returned_data:
				current_users.append(rid[0])
			sql = "INSERT INTO music (user_id,user_name,albums,is_complete) VALUES (%s, %s, %s, %s)"
			user_data = self.User_Consumer()
			band_data = self.Band_Consumer()
			users_to_insert = []
			inserted_users = []
			for user in user_data: ##Iterration inside the user topic data
				if user['user_id'] in current_users: ##Check to not add existing on mysql
					continue
				if user['user_id'] in inserted_users:##Check to not add duplicate users
					continue
				user_dict = {'user_id':user['user_id'],'user_name':user['person']}
				user_albums = []
				user_band_counter = 0
				user_inserted_bands = []
				user_compl = 0 ##Fix for when bands are not published yet
				for band in band_data: ##Iterration inside the band topic data
					if band['band_name'] in user_inserted_bands: ##Fix for duplicate entries on kafka
						continue
					if band['band_name'] in user['bands']:
						user_inserted_bands.append(band['band_name'])
						user_band_counter += 1
						for album in band['albums']:
							user_albums.append(album)
					user_compl = 1 if user_band_counter == 3 else 0
				user_dict['albums'] = str(user_albums)
				user_dict['is_complete'] = user_compl
				users_to_insert.append(user_dict)
				inserted_users.append(user_dict['user_id'])
			if len(users_to_insert) == 0: ##For when all users already exist
				return
			print(f'Users to insert.... {users_to_insert}')
			for new_user in users_to_insert:
				val = (new_user['user_id'], new_user['user_name'], new_user['albums'], new_user['is_complete'])
				cur.execute(sql,val)            
		except Exception as error:
			print(error)
		finally:
			if cur is not None:
				cur.close()
			if myconn is not None:
				myconn.close()

if __name__ == '__main__':
	kafkaConsume = Kafka__Consumer()
	while True:
		kafkaConsume.add_new_user()
		kafkaConsume.update_user()
		time.sleep(5)
