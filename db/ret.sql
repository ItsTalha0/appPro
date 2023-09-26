
 CREATE OR REPLACE FUNCTION GET_FILES_FROM_CHUNKS()
 RETURNS VOID AS 
 $$
 DECLARE
     lmsgid text;
     newoid oid;
     data_offset int;
     nmsgdata bytea;
     cur CURSOR FOR
         WITH cte_msgdata AS (
             SELECT substr(jobdata, 169, encode(substr(jobdata, 159, 10), 'escape')::integer) msgdata
             FROM job_scheduler 
             WHERE encode(substr(jobdata, 123, 36), 'escape') = lmsgid
             AND jstate = 'N-4' 
             AND encode(substr(jobdata, 69, 5), 'escape')::integer = 2
             GROUP BY jobdata
             ORDER BY encode(substr(jobdata, 115, 8), 'escape')::integer
         )
         SELECT msgdata
         FROM cte_msgdata;
 BEGIN
    
     SELECT encode(substr(js1.jobdata, 115, 36), 'escape') msgid
     INTO lmsgid
     FROM job_scheduler js1
     WHERE encode(substr(js1.jobdata, 69, 5), 'escape')::int = 3
     AND js1.jstate = 'N-4'
     AND encode(substr(js1.jobdata, 163, 10), 'escape')::int = (
         SELECT count(js2.jobdata) 
         FROM job_scheduler js2
         WHERE js2.jstate = 'N-4'
         AND encode(substr(js2.jobdata, 69, 5), 'escape')::int = 2
         AND encode(substr(js2.jobdata, 123, 36), 'escape') = encode(substr(js1.jobdata, 115, 36), 'escape')
     );

     newoid := lo_creat(-1);  
     INSERT INTO files (file_id, file_name, file_data) VALUES (gen_random_uuid(), 'new_file_' || gen_random_uuid()::text, newoid);

     OPEN cur;
     LOOP
         FETCH cur INTO nmsgdata;
         EXIT WHEN NOT FOUND;
        
         data_offset := length(lo_get(newoid));
         PERFORM lo_put(newoid, data_offset, nmsgdata);
     END LOOP;
     CLOSE cur;

 END;
 $$ LANGUAGE plpgsql;
 select GET_FILES_FROM_CHUNKS();

 create table temp_data_info(tmsgid uuid PRIMARY KEY, 
                             datasize int NOT NULL, 
                             chunk_count int NOT NULL, 
                             source_name text NOT NULL);

 create table temp_chunks(tmsgid uuid REFERENCES temp_data_info(tmsgid) ON UPDATE CASCADE ON DELETE CASCADE, 
                         tdata bytea NOT NULL, 
                         tchunk_number int NOT NULL);

 with cte_arrnewmsg as (
     select distinct encode(substr(js1.jobdata, 115, 36), 'escape')::uuid msgid,
     encode(substr(js1.jobdata, 151, 12), 'escape')::int msg_size,
     encode(substr(js1.jobdata, 163, 10), 'escape')::int chunk_count,
     encode(substr(js1.jobdata, 74, 5), 'escape') source
     from job_scheduler js1
     where encode(substr(js1.jobdata, 69, 5), 'escape')::int = 3
     and js1.jstate = 'N-4'
     and encode(substr(js1.jobdata, 163, 10), 'escape')::int = (
         select count(js2.jobdata) 
         from job_scheduler js2
         where js2.jstate = 'N-4'
         and encode(substr(js2.jobdata, 69, 5), 'escape')::int = 2
         and encode(substr(js2.jobdata, 123, 36), 'escape') = encode(substr(js1.jobdata, 115, 36), 'escape')
     )
 )
 Insert into temp_data_info select msgid, msg_size, chunk_count, source from cte_arrnewmsg;


 with cte_arrnewmsg as (
     select distinct encode(substr(js1.jobdata, 115, 36), 'escape') msgid
     from job_scheduler js1
     where encode(substr(js1.jobdata, 69, 5), 'escape')::int = 3
     and js1.jstate = 'N-4'
     and encode(substr(js1.jobdata, 163, 10), 'escape')::int = (
         select count(js2.jobdata) 
         from job_scheduler js2
         where js2.jstate = 'N-4'
         and encode(substr(js2.jobdata, 69, 5), 'escape')::int = 2
         and encode(substr(js2.jobdata, 123, 36), 'escape') = encode(substr(js1.jobdata, 115, 36), 'escape')
     )
 ),
 cte_msgdata as (
     select substr(js.jobdata, 169,  encode(substr(js.jobdata, 159, 10), 'escape')::integer) msgdata,
     cte_arrnewmsg.msgid,
     encode(substr(js.jobdata, 115, 8), 'escape')::integer chunk_num 
     from job_scheduler js
     join cte_arrnewmsg 
     on encode(substr(js.jobdata, 123, 36), 'escape') = cte_arrnewmsg.msgid
     where js.jstate = 'N-4' 
     and encode(substr(js.jobdata, 69, 5), 'escape')::integer = 2
     GROUP by js.jobdata, cte_arrnewmsg.msgid
     order by encode(substr(js.jobdata, 115, 8), 'escape')::integer
 )
 INSERT INTO temp_chunks 
 SELECT msgid::uuid, msgdata, chunk_num  
 FROM cte_msgdata;


 with cte_arrnewmsg as (
     select distinct encode(substr(js1.jobdata, 115, 36), 'escape') msgid
     from job_scheduler js1
     where encode(substr(js1.jobdata, 69, 5), 'escape')::int = 3
     and js1.jstate = 'N-4'
     and encode(substr(js1.jobdata, 163, 10), 'escape')::int = (
         select count(js2.jobdata) 
         from job_scheduler js2
         where js2.jstate = 'N-4'
         and encode(substr(js2.jobdata, 69, 5), 'escape')::int = 2
         and encode(substr(js2.jobdata, 123, 36), 'escape') = encode(substr(js1.jobdata, 115, 36), 'escape')
     )
 )
 update job_scheduler 
 set jstate = 'C'
 where encode(substr(jobdata, 123, 36), 'escape') = (select msgid from cte_arrnewmsg)
 or encode(substr(jobdata, 115, 36), 'escape') = (select msgid from cte_arrnewmsg);





