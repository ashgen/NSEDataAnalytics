###############Author: Tradelab Software (P) Ltd.##################### 

import binascii
import struct
import ctypes
import socket
import sys,time
from binascii import unhexlify

class OrderMethods :
	#const = zero_constants()
	def get_header(self,length,messagecode) :
		checksum = 255
		errorCode = 0
		time = 0
		packed_data = ctypes.create_string_buffer(14)  
		struct.pack_into('h',packed_data,0,checksum)
		struct.pack_into('h',packed_data,2,length)
		struct.pack_into('h',packed_data,4,messagecode)
		struct.pack_into('I',packed_data,6,errorCode)
		struct.pack_into('I',packed_data,10,time)
		print binascii.hexlify(packed_data)
		return packed_data
		#"ff000e0021030000000000000000"
	
	def get_cash(self,socket) :
		header = self.get_header(14,801)
		socket.sendall(header)
		print "packet sent"
		packet, address = socket.recvfrom( 50 )
		print "recieving"
		#print len(packet[2:4])
		balance = packet[42:50]
		#value = struct.unpack('s', len)[0]
		balance = struct.unpack('d',balance)
		return balance[0]
		
	def copy_test(self) :
		packed_data = ctypes.create_string_buffer(28)
		header = self.get_header(243,101)
		packed_data[0:14] = header
		print binascii.hexlify(packed_data)
		
		
	def create_order_packet(self,quantity,action,type,price) :
		packed_data = ctypes.create_string_buffer(243)
		header = self.get_header(243,101)
		packed_data[0:14] = header
		symbol = 'NIFTY15SEP8500CE'
		orderID = '1'
		strategy_name = 'name'
		product_type = 'NRML'
		validity = 'DAY'
		side = 0
		if action == 'BUY' :
			side = 1
		else : 
			side = 2
		
		symbol = symbol.ljust(64,'|')
		orderID = orderID.ljust(10,'\0')
		strategy_name = strategy_name.ljust(10,'\0')
		product_type = product_type.ljust(12,'\0')
		validity = validity.ljust(5,'\0')
		
		struct.pack_into('10s',packed_data,14,self.const.exchange) 
		struct.pack_into('64s',packed_data,24,symbol) 
		struct.pack_into('10s',packed_data,108,orderID)
		struct.pack_into('10s',packed_data,118,strategy_name)
		struct.pack_into('h',packed_data,128,side)
		struct.pack_into('I',packed_data,130,quantity)
		struct.pack_into('d',packed_data,142,price)
		struct.pack_into('d',packed_data,150,0)
		struct.pack_into('12s',packed_data,190,type)
		struct.pack_into('12s',packed_data,202,product_type)
		struct.pack_into('12s',packed_data,214,self.const.client_ID)
		struct.pack_into('5s',packed_data,226,validity)
		print >>sys.stderr, 'sending "%s"' % binascii.hexlify(packed_data)
		return packed_data
	

	def get_cash(self,socket) :
		header = self.get_header(14,801)
		socket.sendall(header)
		print "packet sent"
		packet, address = socket.recvfrom( 50 )
		print "recieving"
		#print len(packet[2:4])
		balance = packet[42:50]
		#value = struct.unpack('s', len)[0]
		balance = struct.unpack('d',balance)
		return balance[0]
		
	def get_trades(self,socket) :
		header = self.get_header(14,601)
		socket.sendall(header)
		print "packet sent"
		
		
		header1 = socket.recv(14)  #recieve header packet
		size = header1[2:4] #size of whole packet= 14
		size = struct.unpack('h',size)

		msg = header1[4:6] #messagecode
		msg = struct.unpack('h',msg)
		
		print "receiving Trade Start"
		print str(msg[0]) +" " + str(size[0])
			
		if msg[0] != 602 :
			print "Error in Trade Start"
			return -1
		
		
		while True :
			header2 = socket.recv(14)
			size2 = header2[2:4] #size of whole packet= 14 +229
			size2 = struct.unpack('h',size2)

			msg2 = header2[4:6] #messagecode
			msg2 = struct.unpack('h',msg2)
			
			if msg2[0] == 604 :
				print msg2[0]
				print "Trade Process End"
				return 0
			elif msg2[0] == 603:
				print "\n"
				#print " receiving Trade Process"
			else :
				print msg2[0]
				print "Trade Process Error"
				return 0
				
			packet = socket.recv(size2[0]-14) #recieve Order packet
			print "receiving Trade Response"
			
			print str(msg2[0]) +" " + str(size2[0])
			
			print packet
			exch=packet[0:10] #0=14-14, 10=24-14
			exch = struct.unpack('10s', exch)[0]
		
			initqty=packet[130-14:134-14]
			initqty=struct.unpack('I', initqty)

			avtprice=packet[174-14:182-14]
			avtprice= struct.unpack('d', avtprice)
		
			ordertype=packet[190-14:202-14]
			ordertype= struct.unpack('12s', ordertype)[0]
			print "\nSample::" + exch + " " + str(initqty[0])+" "+str(avtprice[0]) +" " + ordertype
			print "=========================================================="
       
		return -1
		
		
	def get_pending(self,socket) :
		
			header = self.get_header(14,701)
			socket.sendall(header)
			print "packet sent"
		
			header1 = socket.recv(14)  #recieve header packet
			size = header1[2:4] #size of whole packet= 14
			size = struct.unpack('h',size)
		
			msg = header1[4:6] #messagecode
			msg = struct.unpack('h',msg)
		
			print "receiving Pending Orders Start"
			print str(msg[0]) +" " + str(size[0])
			
			if msg[0] != 702 :
				print "Error in Pending Orders Start"
				return -1
		
		
			while True :
				header2 = socket.recv(14)
				size2 = header2[2:4] #size of whole packet= 14 +229
				size2 = struct.unpack('h',size2)

				msg2 = header2[4:6] #messagecode
				msg2 = struct.unpack('h',msg2)
			
				if msg2[0] == 704 :
					print msg2[0]
					print "Pending Orders Process End"
					return 0
				elif msg2[0] == 703:
					print "\n"
					#print " receiving Pending Orders Process"
				else :
					print msg2[0]
					print "Pending Orders Process Error"
					return 0
				
				packet = socket.recv(size2[0]-14) #recieve Order packet
				print "receiving Pending Orders Response"
			
				print str(msg2[0]) +" " + str(size2[0])
				print packet
				exch=packet[0:10] #0=14-14, 10=24-14
				exch = struct.unpack('10s', exch)[0]
		
				initqty=packet[130-14:134-14]
				initqty=struct.unpack('I', initqty)

				lprice=packet[142-14:150-14]
				lprice= struct.unpack('d', lprice)
		
				ordertype=packet[190-14:202-14]
				ordertype= struct.unpack('12s', ordertype)[0]
				print "\nSample::" + exch + " " + str(initqty[0])+" "+str(lprice[0]) +" " + ordertype
				print "=========================================================="
				
		#return -1
		
if __name__ == "__main__" :
	SERVER_IP = 'localhost' 
	SERVER_PORT = 18579
	_clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	server_address = (SERVER_IP, SERVER_PORT)
	print "connecting to server"
	_clientSocket.connect(server_address)
	print "connnected to server"
	#print "Get Trades:::The success code is "+str(OrderMethods().get_trades(_clientSocket))
	print "Get Pending Orders::The success code is "+str(OrderMethods().get_pending(_clientSocket))
	
	#print "The packet length  is "+str(OrderMethods().get_cash(_clientSocket))
	#order_Barray = OrderMethods().create_order_packet(25,'BUY','MKT',0)
	#_clientSocket.sendall(order_Barray)
	_clientSocket.close()
    
