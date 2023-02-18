-- UPDATE SINGLE WRONG VEHICLE
DO
$do$
DECLARE
	cur_imei CURSOR
	FOR
		SELECT imei FROM kharkiv.vehicle
		WHERE name = 'None'
			AND imei IN (
				SELECT imei FROM kharkiv.vehicle
				GROUP BY imei HAVING count(imei) = 1);

BEGIN
	RAISE NOTICE 'UPDATE kharkiv.vehicle started at: % ' , now();

	FOR recordvar IN cur_imei LOOP
		RAISE NOTICE '%\n' , recordvar.imei;
		UPDATE kharkiv.vehicle
			SET name = 'NULL'
			WHERE imei = recordvar.imei;
	END LOOP;

	RAISE NOTICE 'End Time: % ', now();
EXCEPTION
	WHEN OTHERS THEN
	RAISE NOTICE 'UPDATE kharkiv.vehicle failed for :: % ', SQLERRM;
	ROLLBACK;
END;
$do$

-- TRANSFER DATA FROM WRONG VEHICLE TO CORRECT AND DELET WRONG ONE
DO
$do$
DECLARE
	cur_imei CURSOR
	FOR
		SELECT imei FROM kharkiv.vehicle
		GROUP BY imei HAVING count(imei) = 2;

	tmp_cursor refcursor;

	rec1 RECORD;
	rec2 RECORD;
	rec_tmp RECORD;

	cnt_none_befor RECORD;
	cnt_none_after RECORD;
	cnt_null_befor RECORD;
	cnt_null_after RECORD;

BEGIN
	RAISE NOTICE 'UPDATE kharkiv.vehicle started at: % ' , now();

	FOR recordvar IN cur_imei LOOP
		RAISE NOTICE '%' , recordvar.imei;

		SELECT * INTO rec1 FROM kharkiv.vehicle
		WHERE name = 'None' AND imei=recordvar.imei;

		SELECT * INTO rec2 FROM kharkiv.vehicle
		WHERE name = 'NULL' AND imei=recordvar.imei;

		SELECT count(*) as cnt INTO cnt_none_befor FROM kharkiv.gpsdata
			WHERE VEHICLE_ID = rec1.id;
		SELECT count(*) as cnt INTO cnt_null_befor FROM kharkiv.gpsdata
			WHERE VEHICLE_ID = rec2.id;

		OPEN tmp_cursor for
			SELECT * FROM kharkiv.gpsdata
			WHERE VEHICLE_ID = rec1.id;

		<<avl_loop>>
		LOOP
			FETCH tmp_cursor INTO rec_tmp;

			EXIT avl_loop WHEN NOT FOUND;

			BEGIN
				UPDATE kharkiv.gpsdata
					SET VEHICLE_ID = rec2.id
					WHERE ID = rec_tmp.id;
			EXCEPTION WHEN unique_violation THEN
				DELETE FROM kharkiv.gpsdata WHERE ID = rec_tmp.id;
			END;
	   END LOOP avl_loop;
	   CLOSE tmp_cursor;

		SELECT count(*) as cnt INTO cnt_none_after FROM kharkiv.gpsdata
			WHERE VEHICLE_ID = rec1.id;
		SELECT count(*) as cnt INTO cnt_null_after FROM kharkiv.gpsdata
			WHERE VEHICLE_ID = rec2.id;

		RAISE NOTICE '% - % -> % | %' , rec1, cnt_none_befor, cnt_none_after, cnt_none_befor.cnt - cnt_none_after.cnt;
		RAISE NOTICE '% - % -> % | %' , rec2, cnt_null_befor, cnt_null_after, cnt_null_befor.cnt - cnt_null_after.cnt;

		DELETE FROM kharkiv.vehicle WHERE ID = rec1.id;

	END LOOP;

	RAISE NOTICE 'End Time: % ', now();
EXCEPTION
	WHEN OTHERS THEN
	RAISE NOTICE 'UPDATE kharkiv.vehicle failed for :: % ', SQLERRM;
	ROLLBACK;
END;
$do$