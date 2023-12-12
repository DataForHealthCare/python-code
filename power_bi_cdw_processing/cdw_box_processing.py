#!/proj/data/shared/p360-python/bin/python3

from datetime import datetime, timedelta
import cx_Oracle
import subprocess
import pandas as pd
import sys
sys.path.append('/proj/data/shared/bin/python-utils')
import credentials


def main():
    """Credentials"""
    db_user = sys.argv[1]
    db_nm = sys.argv[2]
    db_pw = credentials.get_credentials(db_user, db_nm)

    """Test Connection"""
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

    """CDW Box Jobs"""
    DS_jobs = (
            'bDropCdwIndex',
            'bCreateOncology1_2Index',
            'bCdwPatientGeocode',
            'bInpatientFlowsheetMeasLoad',
            'bCreatePhq9Index',
            'bCreateCdwServCatIndex',
            'bCreateActPatIndex',
            'bCreateAdmissionIndex',
            'bCreateMedicationIndex',
            'bCreateStorkIndex'
        )

    """Compiled Data List"""
    compiled_list_ = []

    for j in DS_jobs:
        result = subprocess.run(['dsrt', j], stdout=subprocess.PIPE, universal_newlines=True)

        out_put = result.stdout.splitlines()

        list_ = [i.split() for i in out_put]

        compiled_list_ += list_

    """DataFrame for Analysis"""
    df = pd.DataFrame(compiled_list_, columns = [
        'job_name', 
        'start_date', 
        'start_time', 
        'start_day', 
        'end_date', 
        'end_time', 
        'end_day', 
        'elapsed_time', 
        'project', 
        'status', 
        'type', 
        'job_name_'
        ], dtype=float)

    s = df.loc[(df['job_name'] == 'bDropCdwIndex') & (df['status'] == 'Finished')].values.tolist()
    e = df.loc[(df['job_name'] != 'bDropCdwIndex') & (df['status'] == 'Finished')].values.tolist()

    date_format_str = '%Y-%m-%d %H:%M:%S'

    et_jobs = []

    for i in s:
        cdw_jobs = []
        for x in e:
            start = datetime.strptime(i[1] + ' ' + i[2], date_format_str)
            end = datetime.strptime(i[1] + ' ' + i[2], date_format_str)
            end_date = end + timedelta(days=5)
            between = datetime.strptime(x[4] + ' ' + x[5], date_format_str)

            if start < between <= end_date:
                cdw_jobs.append([x[0], x[4], x[5], (between - start), x[6]])

        m = max([j[3] for j in cdw_jobs])
   
        m_ = str(m).replace(',', ' and')

        et_jobs.append([i[0], i[1], i[2], i[3]] + [i[:5] for i in cdw_jobs if i[3] == m] + [str(m_)])

        cdw_jobs = []

    cdw_completed_lst = [i[1:4] + i[4][1:3] + [i[4][4]] + [i[5]] for i in et_jobs]
    
    cdw_df = pd.DataFrame(cdw_completed_lst, columns = [
        'start_date', 
        'start_time', 
        'start_day', 
        'end_date', 
        'end_time', 
        'end_day', 
        'total_elapsed_time'
        ], dtype = int)

    # Add Run Data Field 
    cdw_df["run_data"] = ''

    """INSERT INTO Table OPRTNS.cdwprd_box_processing_log"""
    rows = [tuple(x) for x in cdw_df.values.tolist()]

    cur.execute("""TRUNCATE TABLE OPRTNS.cdwprd_box_processing_log""")

    cur.executemany("""INSERT INTO OPRTNS.cdwprd_box_processing_log(
        start_date_txt, 
        start_time_txt, 
        start_day_txt, 
        end_date_txt, 
        end_time_txt, 
        end_day_txt, 
        elapsed_time_txt, 
        run_data_txt, 
        sequence_num) 
        VALUES(:0, :1, :2, :3, :4, :5, :6, :7, OPRTNS.cdw_box_processing_seq.nextval)""", rows)

    connection.commit() 
    connection.close()

if __name__ == '__main__':
    main()