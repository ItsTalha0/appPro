CC = gcc
FS_DIR = $(shell pwd)
LOCAL_INCLUDE_DIR = $(FS_DIR)/include
PSQL_INCLUDE_DIR = /usr/include/postgresql

CFLAGS = -I$(LOCAL_INCLUDE_DIR) -I$(PSQL_INCLUDE_DIR) -lpq -g  -Wall -Wextra -lpthread -mcmodel=large 

processor_r_target = processor_r
processor_r_files := processor_r.c shared_memory.c partition.c storelog.c

processor_s_target = processor_s
processor_s_files := processor_s.c shared_memory.c partition.c storelog.c

notif_target = notif
notif_files := notif.c storelog.c

sender_target = sender
sender_files := sender.c shared_memory.c partition.c storelog.c 

initial_receiver_target = initial_receiver
initial_receiver_files := initial_receiver.c storelog.c

initial_sender_target = initial_sender
initial_sender_files := initial_sender.c storelog.c

receiver_target = receiver
receiver_files := receiver.c shared_memory.c partition.c hashtable.c storelog.c

launcher_target = launcher
launcher_files := launcher.c 

user_program_target = user_program
user_program_files := user_program.c

job_launcher_target = job_launcher
job_launcher_files := job_launcher.c storelog.c

launcher:
	$(CC) $(processor_r_files) -o $(processor_r_target) $(CFLAGS)
	$(CC) $(processor_s_files) -o $(processor_s_target) $(CFLAGS) 
	$(CC) $(notif_files) -o $(notif_target) $(CFLAGS)
	$(CC) $(sender_files) -o $(sender_target) $(CFLAGS)
	$(CC) $(receiver_files) -o $(receiver_target) $(CFLAGS)
	$(CC) $(initial_receiver_files) -o $(initial_receiver_target) $(CFLAGS)
	$(CC) $(initial_sender_files) -o $(initial_sender_target) $(CFLAGS)
	$(CC) $(launcher_files) -o $(launcher_target) $(CFLAGS)
	$(CC) $(user_program_files) -o $(user_program_target) $(CFLAGS)
	$(CC) $(job_launcher_files) -o $(job_launcher_target) $(CFLAGS)

clean:
	rm processor_r processor_s receiver sender notif launcher user_program initial_receiver initial_sender job_launcher storelog
