Step 1)
Navigate to C:\Zerodha\Pi\Data Feed\Omne
Open piclient.ini in notepad and change [Pi_Puser] to TRUE from FALSE as follows
[Pi_Puser]
VALUE=TRUE

Step 2)
Open Pi(You could see now Pi Bridge icon in red at bottom corner) ,Goto View->User Settings->PiBridge tab select "2 way Semi Auto"

Step 3)
If You have Python Installed on your machine,directly call Sample_Placing Order.py from command line ,also the PiBridge icon has turned green
(indicating the connection between Pi and python program is successful) 

Step 4)You can see the sent order signals in Alerts->Generated Alerts window in Pi.

####Addtional Steps for new program/Language#####
Pre-Requisite:Socket Programming knowledge in the language chosen

Steps 5)Refer PiBridge_packet documentation_.pdf to write your own customized socket program in your favorite language.
(Also refer to the sample python code to check for the possible values in the structure used)