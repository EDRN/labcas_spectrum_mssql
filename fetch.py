import pyodbc
import pandas as pd
import os
import boto3

s3 = boto3.resource('s3')

def process_filename(fname):
    return fname.replace(' ','_').replace('-_','-')

def pull_from_s3(s3_loc, file_loc):
    s3_loc="UCDavis_Pathology/"+s3_loc
    try:
        print('--------fetching from s3',s3_loc)
        s3.Bucket("nlstc-data").download_file(s3_loc, file_loc)
    except:
        print('----------problem fetching file from s3:',s3_loc)
    return

def create_xml_met(metadata):
    xml_met="<cas:metadata xmlns:cas=\"http://oodt.jpl.nasa.gov/1.0/cas\">\n"

    for key_val in metadata:
        key = key_val[0]
        val = key_val[1]
        if val==None:
            val=''
        xml_met+="<keyval type=\"vector\"><key>_File_labcas.pathology:"+key+"</key><val>"+str(val)+"</val></keyval>\n"

    xml_met+="<cas:metadata>"

    return xml_met


def create_connection(username, password, server, port):
    cnxn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+','+port+';UID='+username+';PWD='+password)
    return cnxn

def getdataframe(command, cursor):
    df=pd.DataFrame()
    cursor.execute(command)
    rows = [list(row) for row in cursor.fetchall()]
    columns = [column[0] for column in cursor.description]

    for row in rows:
        dict={c:r for c,r in zip(columns,row)}
        df=df.append(dict,ignore_index=True)
    return df

def exploretable(cursor, tablename):
    cursor.execute("SELECT TOP(1) * FROM "+tablename+";")
    rows = [list(row) for row in cursor.fetchall()]
    columns = [column[0] for column in cursor.description]
    print(tablename)
    print(columns)
    print(rows)


def create_dirs(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return

def write_dir_metadata(path, key_val):
    met_file = open(path+ '/' + path.split('/')[-1]+'.cfg', 'w')
    for key, value in key_val.items():
        if value==None:
            value=''
        met_file.write(key + '=' + str(str(value).encode('utf-8').strip())+'\n')
        met_file.flush()
    met_file.close()

def write_file_metadata(path, metadata):
    met_file = open(path + '.xmlmet', 'a')
    met_file.write(create_xml_met(metadata))
    met_file.close()

def create_study_tree(cursor, base_dir, studyName):
    command = "select * from Course where CourseName = '" + studyName + "' ;"
    studies = getdataframe(command, cursor)
    for _, study in studies.iterrows():
        print('study', study['CourseName'])
        studyId = study['Id']
        studyName = process_filename(study['CourseName'])
        base_dir_study = base_dir + '/' + studyName
        create_dirs(base_dir_study)
        write_dir_metadata(base_dir_study, dict(study))
        command = "select * from Lesson where ParentId = " + str(studyId) + " ;"
        experiments = getdataframe(command, cursor)

        for _, experiment in experiments.iterrows():
            print('--experiment', experiment['LessonName'])
            experimentId = experiment['Id']
            experimentName = process_filename(experiment['LessonName'])
            base_dir_exp = base_dir_study + '/' + experimentName
            create_dirs(base_dir_exp)
            write_dir_metadata(base_dir_exp, dict(experiment))
            command = "select * from Specimen where ParentId = " + str(experimentId) + " ;"
            specimens = getdataframe(command, cursor)

            for _, specimen in specimens.iterrows():
                print('----specimen', specimen['AccessionNumber'])
                specimenId = specimen['Id']
                AccessionNumber = process_filename(specimen['AccessionNumber'])
                base_dir_spec = base_dir_exp + '/' + AccessionNumber
                create_dirs(base_dir_spec)
                write_dir_metadata(base_dir_spec, dict(specimen))
                command = "select * from Slide where ParentId = " + str(specimenId) + " ;"
                slides = getdataframe(command, cursor)

                for _, slide in slides.iterrows():
                    # print('------slide', dict(slide))
                    # get the image metadata
                    slideId=slide['Id']
                    print('------slide', slideId)
                    command="select * from Image where ParentId="+str(slideId)+";"
                    image=[row for _,row in getdataframe(command, cursor).iterrows()]

                    if len(image)==0:
                        print('--------no image metadata found!')
                        continue

                    image=image[0]
                    slideName =process_filename(image['CompressedFileLocation'].split('\\')[-1])
                    slide_path = base_dir_spec + '/' + slideName

                    # pull the slide/image from s3 bucket at this location
                    pull_from_s3(studyName+'/'+experimentName+'/'+'1'+'/'+slideName, base_dir_spec + '/' + slideName)

                    # merge the metatdata from Image and Slide tables:

                    file_metadata=[]
                    for k,v in dict(slide).items():
                        file_metadata.append((k,v))
                    for k,v in dict(image).items():
                        file_metadata.append((k,v))

                    write_file_metadata(slide_path, file_metadata)


def fetch_all_table_names(cursor):
    command="SELECT name FROM sysobjects WHERE xtype='U';"
    cursor.execute(command)
    rows = [row[0] for row in cursor.fetchall()]
    return rows




# example code:

server="spectrum.ucdavis.edu"
port='5555'
user=''
password=''

cnxn=create_connection(user,password,server,port)
cursor = cnxn.cursor()


base_dir='/efs/labcas/mcl/archive'
studyName='1.0 Athena- IHC4 with Swedish Breast Cancer Study'
create_study_tree(cursor,base_dir,studyName)



cnxn.close()
