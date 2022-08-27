# importing Libraries
import pandas as pd
import numpy as np
import os
import openpyxl
import sqlalchemy
import pyodbc

DATA_DATABASE_URI = 'mssql+pyodbc://Vaibhav:Bombay123@localhost/Pharma1?charset=latin&driver=ODBC+Driver+17+for+SQL+Server'
def create_engine():
    return sqlalchemy.create_engine(DATA_DATABASE_URI)

data_db = create_engine()


#Reading raw data
Raw_data = pd.read_excel(r"C:\Users\Vaibhav\Desktop\ML Project\ETL Project\Pharma_Data.xlsx")
# print(list(Raw_data.columns))
Raw_data = Raw_data[['Admission_date', 'Patient_Id', 'Patient_First_Name', 'Patient_Last_Name', 'Disease_Code', 'Dignosis_Code', 'Doctors_Id', 'Doctors_Name', 'Specilization', 'Doctors_Experience', 'Patient_Height_cm', 'Patient_Weight_Kg', 'Is_Curable', 'Is_Tested_for_COVID', 'Is_COVID_Positive']]
Raw_data.to_sql('Raw_File_Columns', data_db, if_exists= 'replace', index= False)
Dignosis_Mapping = pd.read_excel(r"C:\Users\Vaibhav\Desktop\ML Project\ETL Project\Dignosis_Code_Mapping.xlsx")
Disease_Mapping = pd.read_excel(r"C:\Users\Vaibhav\Desktop\ML Project\ETL Project\Disease_Code_Mapping.xlsx")
# Normalising the raw data
# Validation Table
Validation_Table =  Raw_data[Raw_data['Admission_date'].isnull()]
Validation_Table.to_sql('Validation_Table', data_db, if_exists= 'replace', index= False)
# print(list(Validation_Table.columns))
# print(Validation_Table.shape)

# print(Validation_Table)

# Removed Null Admission Dates
Raw_data = Raw_data[Raw_data['Admission_date'].notnull()]
Raw_data['Admission_date'] = pd.to_datetime(Raw_data['Admission_date'], format= "%Y/%m/%d")
Raw_data['Is_Tested_for_COVID']= Raw_data.apply(lambda x: 'Y' if x['Is_Tested_for_COVID'] == True
                                                      else 'N', axis= 1)
Raw_data['Is_COVID_Positive']= Raw_data.apply(lambda x: 'Y' if x['Is_COVID_Positive'] == True
                                                      else 'N', axis= 1)
Raw_data['Is_Curable']= Raw_data.apply(lambda x: 'Y' if x['Is_Curable'] == True
                                                      else 'N', axis= 1)

Raw_data = pd.merge(Raw_data, Disease_Mapping, how = 'left', on= 'Disease_Code')
# print(Raw_data.shape)
Raw_data = pd.merge(Raw_data, Dignosis_Mapping, how = 'left', on= 'Dignosis_Code')
# print(Raw_data)

# Old Patient Table
Record_count = Raw_data.Patient_Id.value_counts()
Old_Patient = Raw_data[Raw_data.Patient_Id.isin(Record_count.index[Record_count.gt(1)])]
Old_Patient = Old_Patient.loc[Old_Patient.groupby('Patient_Id').Admission_date.idxmax()]
# print(Old_Patient)
# Old_Patient.drop(Old_Patient.columns[['Disease_Code', 'Dignosis_Code']], axis=1, inplace=True)
# Old_Patient = Old_Patient.drop(columns=['Disease_Code', 'Dignosis_Code'])
# del Old_Patient['Disease_Code']
# del Old_Patient['Dignosis_Code']
Old_Patient.drop(columns=['Disease_Code', 'Dignosis_Code', 'Doctors_Id', 'Specilization', 'Doctors_Name',
                        'Doctors_Experience', 'Is_Tested_for_COVID', 'Is_COVID_Positive'  ], inplace=True)


print(Old_Patient.shape)
Old_Patient.to_sql('Old_Patient', data_db, if_exists= 'replace', index= False)



# print(Old_Patient)


# New Patient Data

New_Patient = Raw_data.groupby('Patient_Id').filter(lambda x: x['Patient_Id'].shape[0]==1)

New_Patient.drop(columns=['Disease_Code', 'Dignosis_Code', 'Doctors_Id', 'Specilization', 'Doctors_Name',
                        'Doctors_Experience', 'Is_Tested_for_COVID', 'Is_COVID_Positive'  ], inplace=True)

New_Patient.to_sql('New_Patient', data_db, if_exists= 'replace', index= False)
# print(New_Patient)

# Cancer_Patients Table

Cancer_Patients = Raw_data[['Admission_date','Patient_Id', 'Patient_First_Name', 'Patient_Last_Name',
                            'Disease_Name', 'Dignosis_Name', 'Doctors_Name', 'Patient_Height_cm', 'Patient_Weight_Kg']]
Cancer_Patients.to_sql('Cancer_Patients', data_db, if_exists= 'replace', index= False)
# Cancer_Patients = pd.read_sql("select * from Cancer_Patients", data_db )
Cancer_Patients = pd.read_sql("select * from Cancer_Patients where Disease_Name = 'Cancer'", data_db)
# print(Cancer_Patients)
Cancer_Patients.to_sql('Cancer_Patients', data_db, if_exists= 'replace', index= False)
# COVID_Positive_Patients Table

COVID_Positive_Patients = Raw_data[['Admission_date', 'Patient_Id', 'Patient_First_Name', 'Patient_Last_Name', 'Is_COVID_Positive']]
COVID_Positive_Patients = COVID_Positive_Patients.loc[COVID_Positive_Patients['Is_COVID_Positive'] == 'Y']
# print(COVID_Positive_Patients)
COVID_Positive_Patients.to_sql('COVID_Positive_Patients', data_db, if_exists= 'replace', index= False)

# Doctor's_Details Table

Doctors_Details = Raw_data[['Doctors_Id', 'Doctors_Name', 'Specilization', 'Doctors_Experience']]
Doctors_details_append = Validation_Table[['Doctors_Id', 'Doctors_Name', 'Specilization', 'Doctors_Experience']]

Doctors_Details = Doctors_Details.append(Doctors_details_append)
Doctors_Details.to_sql('Doctors_Details', data_db, if_exists= 'replace', index= False)

# print(Doctors_Details.shape)





