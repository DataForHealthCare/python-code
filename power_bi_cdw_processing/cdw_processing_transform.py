#!/proj/data/shared/p360-python/bin/python3

import cx_Oracle
import subprocess
import csv
import re
import sys
sys.path.append('/proj/data/shared/bin/python-utils')
import credentials


def main():
    """Credentials"""
    db_user = sys.argv[1]
    db_nm = sys.argv[2]
    db_pw = credentials.get_credentials(db_user, db_nm)

    """Test connection"""
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
    
    """CDW jobs"""
    DS_jobs = [
        'bOncology1Stage',
        'bDropOncology1_2Index',
        'bOncology1Base',
        'bProblemListLoad',
        'bCreateOncology1_2Index',
        'bDropCdwIndex',
        'bCDWBase',
        'bInpatientFlowsheetMeasLoad',
        'bEncTransLoad',
        'bCreateCdwIndex',
        'bAdwCdwPatientMapMain',
        'bDropPhq9Index',
        'bPhq9QuestionResponseDsLoad',
        'bCreatePhq9Index',
        'bDropStageServCatIndex',
        'bServCategoryStage',
        'bCreateStageServCatIndex',
        'bDropCdwServCatIndex',
        'bServCategoryBase',
        'bCreateCdwServCatIndex',
        'bDropStorkIndex',
        'bStorkBase',
        'bCreateStorkIndex',
        'bDropActPatIndex',
        'bActivePatient_Dimload',
        'bActivePatient_Factload',
        'bCreateActPatIndex',
        'bDropAdmissionIndex',
        'bAdmission_Dimload',
        'bAdmission_Factload',
        'bCreateAdmissionIndex',
        'bDropMedicationIndex',
        'bMedication_Dimload',
        'bMedication_Factload',
        'bCreateMedicationIndex',
        'bOpsSendEmail-CDW-Completed'
    ]

    """Create the CSV file"""
    with open('cdw_processing_output.csv', 'w') as f:
        writer = csv.writer(f, delimiter=',')
        for job in DS_jobs:
            result = subprocess.run(['dsrt', job], stdout=subprocess.PIPE, universal_newlines=True)
            out_put = result.stdout.splitlines()
            # Transforming the data
            for i in out_put[1:]:
                pattern = re.compile(r'\s+')
                line = re.sub(pattern, ' ', i)
                l = line.split()
                if l[9] != 'Finished':
                    l[9] = l[11][:-1]
                writer.writerow(l[:-2])

    """Truncate table"""
    cur.execute("""TRUNCATE TABLE OPRTNS.cdwprd_processing_log""")

    """Updating the SQL table""" 
    with open('cdw_processing_output.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            cur.execute("""INSERT INTO OPRTNS.cdwprd_processing_log(
                job_name_txt, 
                start_date_txt, 
                start_time_txt, 
                start_day_txt, 
                end_date_txt, 
                end_time_txt, 
                end_day_txt, 
                elapsed_time_txt, 
                project_name_txt, 
                status_txt,
                record_num)
                VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10, OPRTNS.cdw_processing_seq.nextval)""",
                (
                    str(row[0]),
                    str(row[1]),
                    str(row[2]),
                    str(row[3]),
                    str(row[4]),
                    str(row[5]),
                    str(row[6]),
                    str(row[7]),
                    str(row[8]),
                    str(row[9])
                ))
            connection.commit() 
    connection.close()
    
if __name__ == '__main__':
    main()
