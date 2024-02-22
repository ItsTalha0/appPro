
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
