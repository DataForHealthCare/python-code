#!/proj/data/shared/p360-python/bin/python3

import cx_Oracle
import re

class BatchTracking:
    def __init__(self, py_program = None, db_nm=None, db_user=None, db_pw=None, batch_run_id=None, step_id=None):
        self.py_program = py_program
        self.db_nm = db_nm
        self.db_user = db_user
        self.db_pw = db_pw
        self.batch_run_id = batch_run_id
        self.step_id = step_id

    @classmethod
    def start_batch(cls, py_program, db_nm, db_user, db_pw):
        connection = None
        try:
            connection = cx_Oracle.connect(
                db_user,
                db_pw,
                db_nm
            )
            connection.ping()

        except cx_Oracle.Error:
            print("Connection failed")
            raise

        cur = connection.cursor()

        py_prog = re.search(r'[^/]+$', py_program).group(0)
    
        validate = cur.execute(
            """SELECT * FROM OPRTNS.ETL_BATCH_OPS WHERE batch_name = :py_name""",
            {'py_name': py_prog}
        )

        if len(validate.fetchall()) == 0:
            cur.execute(
                """INSERT INTO OPRTNS.ETL_BATCH_OPS(batch_run_id, batch_name, batch_system_id, insert_ts, mod_ts, delete_ts)
                        VALUES(
                            OPRTNS.ETL_BATCH_SEQ.NEXTVAL, 
                            :py_name, 
                            NULL, 
                            TO_TIMESTAMP(SYSDATE), 
                            TO_TIMESTAMP(SYSDATE), 
                            NULL
                            )""", {'py_name': py_prog})
        else:
            print('Existing BATCH_OPS record')

        batch_run_id = cur.var(cx_Oracle.NUMBER)
        cur.callproc('OPRTNS.ETL_BATCH_PKG.START_ETL_BATCH_RUN', [py_prog, batch_run_id])
    
        return cls(
            py_program, 
            db_nm, 
            db_user, 
            db_pw, 
            batch_run_id
            )

    def stop_batch(self, status_cd='FS'):
        connection = self.oracle_connection()

        cur = connection.cursor()

        cur.callproc('OPRTNS.ETL_BATCH_PKG.SET_ETL_BATCH_RUN_STATUS', [
            int(self.batch_run_id.getvalue()), status_cd])

        connection.commit()
        cur.close()
        connection.close()

    def start_step(self, step_name):
        connection = self.oracle_connection()

        cur = connection.cursor()

        step_id = cur.var(cx_Oracle.NUMBER)

        cur.callproc('OPRTNS.ETL_BATCH_PKG.START_ETL_BATCH_STEP', [
            int(self.batch_run_id.getvalue()), 
            step_name, 
            None, 
            None, 
            None, 
            None, 
            step_id
            ])

        self.step_id = step_id
    
    def stop_step(self, status_cd='FS'):
        connection = self.oracle_connection()

        cur = connection.cursor()

        cur.callproc('OPRTNS.ETL_BATCH_PKG.FINISH_ETL_BATCH_STEP', [
            int(self.step_id.getvalue()), 
            status_cd, 
            None, 
            None, 
            None, 
            None
            ])

    def oracle_connection(self):
        connection = None
        try:
            connection = cx_Oracle.connect(
                self.db_user,
                self.db_pw,
                self.db_nm
            )
            connection.ping()

        except cx_Oracle.Error:
            print("Connection failed")
            raise
        return connection