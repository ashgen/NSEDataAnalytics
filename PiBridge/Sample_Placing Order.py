###############Author: Tradelab Software (P) Ltd.##################### 

import binascii
import struct
import ctypes
import socket
import sys,time


from ctypes import *

class PiBrdgHeader(Structure):
	_pack_ = 1
	_fields_ = [
	('_checksum',c_short),
	('_length',c_short),
	('_msgType',c_short),
	('_errorCode',c_int),
	('_time',c_int)]



class NPiBrdgOrder(Structure):
	_pack_ = 1
	_fields_ = [
	('_header',PiBrdgHeader),
	('_exchange', c_char*10),
	('_trdSymbol', c_char*64),
	('_piOrderId', c_char*20),
	('_orderId', c_char*10),
	('_strategyName', c_char*10),
	('_side',c_short),
	('_initQty',c_int),
	('_disQty',c_int),
	('_remQty',c_int),
	('_lPrice',c_double),
	('_triggerPrice',c_double),
	('_totTradedValue',c_double),
	('_lastTradePrice',c_double),
	('_avgTradePrice',c_double),
	('_tradedQuantity',c_int),
	('_lastTradeQuantity',c_int),
	('_orderType', c_char*12),
	('_prodTypeStr', c_char*12),
	('_ClientCode', c_char*12),
	('_validity', c_char*5),
	('_orderStatus',c_int),
	('_entryTime',c_int),
	('_execTime',c_int)]

SERVER_IP = '127.0.0.1'# it should be IP of local machine
SERVER_PORT = 18579

def SendPktToServerOverTcp(order):
	packed_data = ctypes.create_string_buffer(243)  
	struct.pack_into('h',packed_data,0,order._header._checksum)
	struct.pack_into('h',packed_data,2,order._header._length)
	struct.pack_into('h',packed_data,4,order._header._msgType)
	struct.pack_into('I',packed_data,6,order._header._errorCode)
	struct.pack_into('I',packed_data,10,order._header._time)
	struct.pack_into('10s',packed_data,14,order._exchange) 
	struct.pack_into('64s',packed_data,24,order._trdSymbol) 
	struct.pack_into('20s',packed_data,88,order._piOrderId)
	struct.pack_into('10s',packed_data,108,order._orderId)
	struct.pack_into('10s',packed_data,118,order._strategyName)
	struct.pack_into('h',packed_data,128,order._side)
	struct.pack_into('I',packed_data,130,order._initQty)
	struct.pack_into('I',packed_data,134,order._disQty)
	struct.pack_into('I',packed_data,138,order._remQty)
	struct.pack_into('d',packed_data,142,order._lPrice)
	struct.pack_into('d',packed_data,150,order._triggerPrice)
	struct.pack_into('d',packed_data,158,order._totTradedValue)
	struct.pack_into('d',packed_data,166,order._lastTradePrice)
	struct.pack_into('d',packed_data,174,order._avgTradePrice)
	struct.pack_into('I',packed_data,182,order._tradedQuantity)
	struct.pack_into('I',packed_data,186,order._lastTradeQuantity)
	struct.pack_into('12s',packed_data,190,order._orderType)
	struct.pack_into('12s',packed_data,202,order._prodTypeStr)
	struct.pack_into('12s',packed_data,214,order._ClientCode)
	struct.pack_into('5s',packed_data,226,order._validity)
	struct.pack_into('I',packed_data,231,order._orderStatus)
	struct.pack_into('I',packed_data,235,order._entryTime)
	struct.pack_into('I',packed_data,239,order._execTime)
	print >>sys.stderr, 'sending "%s"' % binascii.hexlify(packed_data)
	_clientSocket.sendall(packed_data)
	
	
def CreateAndReturnPkt():
	order = NPiBrdgOrder()
	order._header._checksum =  255
	order._header._length = 243
	order._header._msgType = 101
	order._header._time = 0
	order._exchange = "NSE"
	order._exchange.ljust(10,'\0')
	order._trdSymbol = "JPASSOCIAT-EQ"
	order._trdSymbol.ljust(64,'|')
	order._orderId = "SAKET_"
	order._orderId.ljust(10,'\0')
	order._strategyName = "STGY1"
	order._strategyName.ljust(10,'\0')
	order._side = 1
	order._initQty = 1 #  ( should be in lots)
	order._disQty = 1 # 
	order._remQty = 1 # 
	order._lPrice = 100 # price in INR
	order._triggerPrice = 10
	order._orderType = "L"
	order._orderType.ljust(12,'\0')
	order._prodTypeStr = "CNC"
	order._prodTypeStr.ljust(12,'\0')
	order._ClientCode = "DN0005"
	order._ClientCode.ljust(12,'\0')
	order._validity = "DAY"
	order._validity.ljust(5,'\0')
	order._orderStatus = 0
	order._entryTime = 0
	order._execTime = 0
	return order



# Create a TCP/IP socket
_clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Connect the socket to the port where the Pi server is listening
server_address = (SERVER_IP, SERVER_PORT)
_clientSocket.connect(server_address)
print "connnected to server"

order = CreateAndReturnPkt()
print "Order price: ", order._lPrice
while True:
	SendPktToServerOverTcp(order)
	time.sleep(200)
