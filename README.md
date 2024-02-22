

--Actuall Readme of the project For the changes made please refer the end of the document.--
This system comprised of different processes is used to send data over network.
System consists 8 different user processes and a database.
Processing of data occurs inside database and user processes are used for sending and receiving data over network.

Words like message, job at its core just represent individual row/s in tables.

Every system listens over two different ports, one fixed port for consensus forming (commsport), another variable port for data communication (dataport), ipaddress of the system remains common in both cases. This data is stored in selfinfo table inside database.
Reason for keeping data port variable is to support future modification where one system could be listening on multiple ports.

In order to send data, two systems form a concensus using the commsport about the size of message and port over which communication will occur. As commsport port is a fixed port, it is taken for granted that all systems must be listening on this port for consensus related
messages. Only ipaddress of the system is required.

Lets suppose there are two systems S1, S2. 
S1 intends to send some data to S2.
To form initial consensus, database of S1 first checks if it has required information realted to the S2. 
If it does, then it will procced with sending the message, if it doesn't then S1's database creates a fixed size message consisting of its ipaddress, dataport and sending capacity. The message is read by intial_sender process of S1 and sends the message over network to S2's commsport, considering the fact that the S1 knows in advance the ipaddress of S2. Message is received by intial_receiver process of S2 and stored in its database. Inside database message capacity of S1 and S2 is compared. 
S2 finalizes the message size based on whichever is smaller among two, S2 stores the information of S1 in sysinfo table. This information is consists of S1's dataport, the size that is finalized by S2 along with the ipaddress and commsport of S1. 
Then S2 prepares a message for S1 consist of its ipaddress, dataport and message size that has been finalized. This message is again sent to S1 via inital_sender process of S2 and received by initial_receiver process of S1. 
Once received S1 updates the information related to S2 in its sysinfo table. Processor_r process of both S1 and S2 will receive a message from their respective databases after insertion of message size in sysinfo table. 
Processor_r process will send that message to receiver process via shared memory. The message is consists of ipadress and message size of the process for whom the message_size has been updated in sysinfo table. Using info from this message receiver process will create a entry in hashtable where key is ipaddress and value is size of message. 
So, whenever a connection is opened on receiver by said ipaddress the receiver can access the message_size instantly and can read the messages of said size from the incoming port where connection has been opened

In order to send message from S1 to S2. First import the file in S1's files table. Then create a job to send the specified file to S2. Both these steps can be done using user program.

Once the job to send the message is created, given there is already a consensus between S1 and S2 on message size, database of S1 will check the file size and split it into ceil(file_size/Message_size) chunks. Then each chunk will gets its own header indicating its serial number and to which message it belongs i.e parent id. 
Along with these chunks a separete message consisting of info related to chunks such has chunk count and original message size is created. Parent id is use to identify chunks belonging to same original message.
After that database will send a message to processor_s process, processor_s will send that message to sender, the message is consist of ip address and port. Sender will try to open a connection on said ipaddress and port. If successfull sender will send a message about status of connection to processor_s which in turn will be stored in database. If connection is successfull the message will consist of file descriptor and ipaddress belonging to that socket or it will consist of -1 and ipaddress. 

If a connection is open to a systme and there are messages that are needed to be sent, then processor_s will read those messages along with file descriptor for that connection socket. The message will be sent to sender via shared memory between the two processes. Sender will try to send the message and send an update about status of message to database. On receveing system's side receiver process will receive the message and read the data, once data equal to the size of message_size is read, then the message is passed to processor_r, processor_r stores the message in database.

Messages inside database goes through various states.
There is a parent message that is present througout the lifetime of system its state always remains in N-O state.
Any newly arrived message is kept in N-1 state.
Hash of message in N-1 state is checked if message matches with hash, then message is promoted to N-2 state.
If message fails to match the hash then it is moved to dead state D.
Destination of message in N-2 state is checked, if the message is intended for some other system, then the message is promoted to S-1 state. If message is intended for same system then it is promoted to N-3 state.
In N-3 state type of message is checked. After identification of type the message is promoted to N-4 state. 
For messages in N-4 state, action is performed on the basis of their type.

Type 1 represents message that has no chunks, means its a single piece message.
Type 2 represents message which is a chunk of some other bigger message.
Type 3 represents message which consist info of chunks of some other bigger message.
Type 4 represents message which is initialy sent to create consensus.
Type 5 represents message which is sent in reply of message of type 4.
Type 6 represents job created to send data from one system to another.

For messages in S-1 state, sysinfo table is checked for message size capacity of destination system. 
If message_size is not available then a job for formation of consensus for message_size is created. 
If the message_size of destination system is available, then size of message is checked if there is need to divided the message in chunks, then the message is promoted to S-2 state. 
For message is S-2 state, chunks are created, all chunks are kept in S-4 state and the main message is sent form S-2 state to S-2W state untill all the chunks are successfully sent, in case of failure in sending chunks, message moves fromm S-2W state to D state i.e dead state. 
Message that fits the message size have no need to split and are directly promoted to S-4 state. 
Processor_r picks up message from S-4 state for whom there is connection open to their destination.
Messages for consensus formig are sent to S-5 state and are picked up by intial_sender.

For any errors that occur in any of the processes, logs entries are directly made in logs table by that process.
Any errors that occur before making connection to database are stored in syslog.





