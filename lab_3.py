from mpi4py import MPI
import json
import hashlib
import os
from collections import defaultdict

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

file_data = {}

if rank == 0:
	if not os.path.exists("map"):
		os.makedirs("map")
	match = 1
	f = open("json.txt", "r")
	file = open('data.txt', 'w+')
	read_json = f.read()
	loaded_json = json.loads(read_json)
	for key, value in loaded_json.items():
		if match == size:
			match=1
		data = {}
		data[key]=value
		request = comm.isend(data, dest=match, tag=11)
		request.wait()
		match = match + 1
	data={}
	
	for i in range(1,size):
		request = comm.isend(data, dest=i, tag=12)
		request.wait()
		
	# TOATE PROCESELE AU TRECUT DE FAZA DE MAPARE
	for i in range(1,size):
		val = comm.recv(source=MPI.ANY_SOURCE, tag=12)
		file_data.update(val)
	
	# URMEAZA ETAPA DE REDUCERE
	for k,v in file_data.items():
		if match == size:
			match=1
		request = comm.isend(k, dest=match, tag=12)
		request.wait()
		match = match + 1
		
	for i in range(1,size):
		request = comm.isend(data, dest=i, tag=13)
		request.wait()
		
	file_data_final = defaultdict(list)
	
	for i in range(1,size):
		val = comm.recv(source=MPI.ANY_SOURCE, tag=13)
		
		for k in val.keys():
			v = val[k]
			for value_v in v:
				file_data_final[file_data[k]].append(file_data[value_v])
	
	f = open("final.txt", "w")
	f.write(json.dumps(file_data_final, indent=4, sort_keys=True))
	f.close()
	
else:
	while 1==1:
		request = comm.irecv(source=0, tag=MPI.ANY_TAG)
		status = MPI.Status()
		data = request.wait(status)
		tag = status.Get_tag()
		if tag==12:
			break
		else:
			for key, value in data.items():
				key_encode = hashlib.md5(key.encode())
				file_data[key_encode.hexdigest()] = key
				for i in value:
					value_encode = hashlib.md5(i.encode())
					file_data[value_encode.hexdigest()] = i
					filename = str(value_encode.hexdigest()) + '_' + str(key_encode.hexdigest()) + '.txt'
					with open(os.path.join("map", filename), 'wb') as aux_file:
						aux_file.write("file".encode())
	request = comm.send(file_data, dest=0, tag=12)
	
	file_data = defaultdict(list)
	
	while 1==1:
		request = comm.irecv(source=0, tag=MPI.ANY_TAG)
		status = MPI.Status()
		data = request.wait(status)
		tag = status.Get_tag()
		print(tag)
		if tag==13:
			break
		else:
			entries = os.scandir('map/')
			for entry in entries:
				if entry.name.split('_')[0] == data:
					file_data[entry.name.split('_')[0]].append(entry.name.split('_')[1].split('.')[0])
	
	request = comm.send(file_data, dest=0, tag=13)
	
print("End.. ", rank)
