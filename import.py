from fastapi import FastAPI, HTTPException
from datetime import datetime
import requests
import mysql.connector as mysql
import json

app = FastAPI()

# Centralized static API URL
API_URL = "http://171.50.175.227:9991/prlws/API_GetAllEmployeeData"
body_params = {
    "APIKey": "141B1E79-0592-4781-9EE1-EFA96EA8F0F6 ",
    "MonthYear": "03/2024"
}
response = requests.post(API_URL, json=body_params)
data = []
if response.status_code == 200:
    try:
        data = response.json()['lstEmployeeData']
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Error parsing API response: {err}")
else:
    raise HTTPException(status_code=response.status_code, detail=f"API request failed with status code: {response.status_code}")

# Centralized function to get the database connection
def get_db_connection():
    try:
        db = mysql.connect(
            host="test-dev-database.cd4qap01cefq.ap-south-1.rds.amazonaws.com",
            database="chandan_retail",
            user="asdfghjkl",
            password="3wTfp93EnqqmZL8m32",
            port="6069"
        )
        return db
    except mysql.Error as err:
        raise HTTPException(status_code=500, detail=f"Database connection error: {err}")

# API endpoint to fetch and store employee basic details
@app.post("/get-employee-data/")
async def get_employee_data():
    if data:
        db = get_db_connection()
        cur = db.cursor(dictionary=True)
        
        for i, employee in enumerate(data):
            EmployeeID = employee['EmployeeID']
            EmployeeName = employee['EmployeeName']
            ShortName = employee['ShortName']
            SalesmanCode = employee['SalesmanCode']
            ThumbNo = employee['ThumbNo']
            SalesmanID = employee['SalesmanID']
            Address1 = employee['Address1']
            Address2 = employee['Address2']
            Address3 = employee['Address3']
            CityName = employee['CityName']
            Pincode = employee['Pincode']
            MobileNo = employee['MobileNo']
            PhoneNo = employee['PhoneNo']
            BirthDate = datetime.strptime(employee['BirthDate'], "%d/%m/%Y").strftime("%Y-%m-%d") if employee['BirthDate'] else None
            JoinDate = datetime.strptime(employee['JoinDate'], "%d/%m/%Y").strftime("%Y-%m-%d") if employee['JoinDate'] else None
            ResignDate = datetime.strptime(employee['ResignDate'], "%d/%m/%Y").strftime("%Y-%m-%d") if employee['ResignDate'] else None
            BankName = employee['BankName']
            BankAcName = employee['BankAcName']
            BankAcCode = employee['BankAcCode']
            BankIFSCCode = employee['BankIFSCCode']
            PanNo = employee['PanNo']
            AdharCardNo = employee['AdharCardNo']
            NetSalaryAnnualAmt = employee['NetSalaryAnnualAmt']
            CTCAnnualAmt = employee['CTCAnnualAmt']
            CompanyContributionAnnualAmt = employee['CompanyContributionAnnualAmt']
            JobSalaryType = employee['JobSalaryType']
            DesignationName = employee['DesignationName']
            PFNo = employee['PFNo']
            ESICNo = employee['ESICNo']
            UANNo = employee['UANNo']
            EmployeeNickName = employee['EmployeeNickName']
            ReportingTo = employee['ReportingTo']
            DepartmentName = employee['DepartmentName']
            MobileNo2 = employee['MobileNo2']
            EmergencyNo = employee['EmergencyNo']
            PayrollStartDate = datetime.strptime(employee['PayrollStartDate'], "%d/%m/%Y").strftime("%Y-%m-%d") if employee['PayrollStartDate'] else None
            Email = employee['Email']
            Remarks = employee['Remarks']
            BondPeriod = employee['BondPeriod']
            GenderType = employee['GenderType']
            MaritalStatus = employee['MaritalStatus']
            EmployeeStatus = employee['EmployeeStatus']
            PermanentAddress1 = employee['PermanentAddress1']
            PermanentAddress2 = employee['PermanentAddress2']
            PermanentAddress3 = employee['PermanentAddress3']
            PermanentCityName = employee['PermanentCityName']
            PermanentPincode = employee['PermanentPincode']
            BranchName = employee['BranchName']

            family_details = employee['FamilyDetails']
            relationship = father_name = husband_name = ''
            if family_details:
                for member in family_details:
                    relationship = member.get('Relationship', '')
                    if relationship:
                        if relationship.lower() == "father":
                            father_name = member.get('MemberName', '')
                        if relationship.lower() == "husband":
                            husband_name = member.get('MemberName', '')

            insert_data = "INSERT INTO employee_data(EmployeeID, EmployeeName, ShortName, SalesmanCode, ThumbNo, SalesmanID, Address1, Address2, Address3, CityName, Pincode, MobileNo, PhoneNo, BirthDate, JoinDate, ResignDate, BankName, BankAcName, BankAcCode, BankIFSCCode, PanNo, AdharCardNo, NetSalaryAnnualAmt, CTCAnnualAmt, CompanyContributionAnnualAmt, JobSalaryType, DesignationName, PFNo, ESICNo, UANNo, EmployeeNickName, ReportingTo, DepartmentName, MobileNo2, EmergencyNo, PayrollStartDate, Email, Remarks, BondPeriod, GenderType, MaritalStatus, EmployeeStatus, PermanentAddress1, PermanentAddress2, PermanentAddress3, PermanentCityName, PermanentPincode, BranchName, Father_Name, Husband_Name, created, modified) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            
            data_to_insert = (EmployeeID, EmployeeName, ShortName, SalesmanCode, ThumbNo, SalesmanID, Address1, Address2, Address3, CityName, Pincode, MobileNo, PhoneNo, BirthDate, JoinDate, ResignDate, BankName, BankAcName, BankAcCode, BankIFSCCode, PanNo, AdharCardNo, NetSalaryAnnualAmt, CTCAnnualAmt, CompanyContributionAnnualAmt, JobSalaryType, DesignationName, PFNo, ESICNo, UANNo, EmployeeNickName, ReportingTo, DepartmentName, MobileNo2, EmergencyNo, PayrollStartDate, Email, Remarks, BondPeriod, GenderType, MaritalStatus, EmployeeStatus, PermanentAddress1, PermanentAddress2, PermanentAddress3, PermanentCityName, PermanentPincode, BranchName, father_name, husband_name, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

            try:
                cur.execute(insert_data, data_to_insert)
                db.commit()
                print("Employee data Inserted....")
            except mysql.Error as err:
                db.rollback()
                print(f"Error: {err} for record {i+1}: {data_to_insert}")
        
        cur.close()
        db.close()

        return {"message": "Employee data processed successfully"}

# API endpoint to fetch and store employee attendance details
@app.post("/get-attendance-data/")
async def get_attendance_data():
    if data:
        db = get_db_connection()
        cur = db.cursor(dictionary=True)
        EmployeeID = AttnDate = DayName = AttendanceType = InTime1 = LastOutTime = AttendanceTypeDesc = EarlyGoingMinute = LateMinute = WorkingMinute = InOutDoor = Lunchbreak = TeaBreak = ''
        for i, attendance in enumerate(data):
            attendance_details = attendance['AttendanceDetails']
            if attendance_details:
                for j, detail in enumerate(attendance_details):
                    EmployeeID = detail.get('EmployeeID', '')
                    AttnDate = datetime.strptime(detail.get('AttnDate', ''), "%d/%m/%Y").strftime("%Y-%m-%d") if detail.get('AttnDate') else None
                    DayName = detail.get('DayName', '')
                    AttendanceType = detail.get('AttendanceType', '')
                    InTime1 = detail.get('InTime1', '')
                    LastOutTime = detail.get('LastOutTime', '')
                    AttendanceTypeDesc = detail.get('AttendanceTypeDesc', '')
                    EarlyGoingMinute = detail.get('EarlyGoingMinute', '')
                    LateMinute = detail.get('LateMinute', '')
                    WorkingMinute = detail.get('WorkingMinute', '')
                    InOutDoor = detail.get('InOutDoor', '')
                    Lunchbreak = detail.get('Lunchbreak', '')
                    TeaBreak = detail.get('TeaBreak', '')

                    insert_data = "INSERT INTO attendance(EmployeeID, AttnDate, DayName, AttendanceType, InTime1, LastOutTime, AttendanceTypeDesc, EarlyGoingMinute, LateMinute, WorkingMinute, InOutDoor, Lunchbreak, TeaBreak, created, modified) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    data_to_insert = (EmployeeID, AttnDate, DayName, AttendanceType, InTime1, LastOutTime, AttendanceTypeDesc, EarlyGoingMinute, LateMinute, WorkingMinute, InOutDoor, Lunchbreak, TeaBreak, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

                    try:
                        cur.execute(insert_data, data_to_insert)
                        db.commit()
                        print("Attendance data inserted....")
                    except mysql.Error as err:
                        print(f"Error: {err} for record {i+1}: {data_to_insert}")

        cur.close()
        db.close()

        return {"message": "Attendance data processed successfully"}

# API endpoint to fetch and store employee basic details
@app.post("/get-leave-data/")
async def get_leave_data():
    if data:
        db = get_db_connection()
        cur = db.cursor(dictionary=True)
        EmployeeID = LeaveDate = LeaveType = IsApproved = ''
        for i, leave in enumerate(data):
            leave_details = leave['LeaveDetails']
            if leave_details:
                for j, detail in enumerate(leave_details):
                    EmployeeID = detail.get('EmployeeID', '')
                    LeaveDate = datetime.strptime(detail.get('LeaveDate', ''), "%d/%m/%Y").strftime("%Y-%m-%d") if detail.get('LeaveDate') else None
                    LeaveType = detail.get('LeaveType', '')
                    IsApproved = detail.get('IsApproved', '')

                    insert_data = "INSERT INTO leave_details(EmployeeID, LeaveDate, LeaveType, IsApproved, created, modified) VALUES(%s, %s, %s, %s, %s, %s)"

                    data_to_insert = (EmployeeID, LeaveDate, LeaveType, IsApproved, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

                    try:
                        cur.execute(insert_data, data_to_insert)
                        db.commit()
                        print("Leave data inserted....")
                    except mysql.Error as err:
                        db.rollback()
                        print(f"Error: {err} for record {j+1}: {data_to_insert}")
        cur.close()
        db.close()

        return {"message": "Leave data processed successfully"}

# API endpoint to fetch and store employee basic details
@app.post("/get-salary-structure/")
async def get_salary_structure():
    if data:
        db = get_db_connection()
        cur = db.cursor(dictionary=True)
        EmployeeID = GrossSalary = BasicSalaryPrc = BasicSalaryAmt = HRAPrc = HRAAmt = DAPrc = DAAmt = LTCPrc = LTCAmt = EducationPrc = EducationAmt = ConveyancePrc = ConveyanceAmt = MgmtPrc = MgmtAmt = DriverPrc = DriverAmt = MedicalAmt = OtherAmt = TotalSalary = PFDeductOn = PFPrc = PFAmt = ESIPrc = ESIAmt = ProfessionTaxAmt = IncomeTaxAmt = DeductOtherAmt = TotalDeductionAmt = CashSalaryAmt = CompanyPFPrc = CompanyPFAmt = CompanyESIPrc = CompanyESIAmt = CompanyBonusPrc = CompanyBonusAmt = PerformanceBonusAmt = TotalCompanyContribution = NetSalary = ctc = FromDate = ToDate = CompanyBonusDeductOn = CompanyEPSPrc = CompanyEPSAmt = CompanyEPFPrc = CompanyEPFAmt = ''
        for i, structure in enumerate(data):
            salary_structure = structure['SalaryStructure']
            if salary_structure:
                for j, detail in enumerate(salary_structure):
                    EmployeeID = detail.get('EmployeeID', '')
                    GrossSalary = detail.get('GrossSalary', '')
                    BasicSalaryPrc = detail.get('BasicSalaryPrc', '')
                    BasicSalaryAmt = detail.get('BasicSalaryAmt', '')
                    HRAPrc = detail.get('HRAPrc', '')
                    HRAAmt = detail.get('HRAAmt', '')
                    DAPrc = detail.get('DAPrc', '')
                    DAAmt = detail.get('DAAmt', '')
                    LTCPrc = detail.get('LTCPrc', '')
                    LTCAmt = detail.get('LTCAmt', '')
                    EducationPrc = detail.get('EducationPrc', '')
                    EducationAmt = detail.get('EducationAmt', '')
                    ConveyancePrc = detail.get('ConveyancePrc', '')
                    ConveyanceAmt = detail.get('ConveyanceAmt', '')
                    MgmtPrc = detail.get('MgmtPrc', '')
                    MgmtAmt = detail.get('MgmtAmt', '')
                    DriverPrc = detail.get('DriverPrc', '')
                    DriverAmt = detail.get('DriverAmt', '')
                    MedicalAmt = detail.get('MedicalAmt', '')
                    OtherAmt = detail.get('OtherAmt', '')
                    TotalSalary = detail.get('TotalSalary', '')
                    PFDeductOn = detail.get('PFDeductOn', '')
                    PFPrc = detail.get('PFPrc', '')
                    PFAmt = detail.get('PFAmt', '')
                    ESIPrc = detail.get('ESIPrc', '')
                    ESIAmt = detail.get('ESIAmt', '')
                    ProfessionTaxAmt = detail.get('ProfessionTaxAmt', '')
                    IncomeTaxAmt = detail.get('IncomeTaxAmt', '')
                    DeductOtherAmt = detail.get('DeductOtherAmt', '')
                    TotalDeductionAmt = detail.get('TotalDeductionAmt', '')
                    CashSalaryAmt = detail.get('CashSalaryAmt', '')
                    CompanyPFPrc = detail.get('CompanyPFPrc', '')
                    CompanyPFAmt = detail.get('CompanyPFAmt', '')
                    CompanyESIPrc = detail.get('CompanyESIPrc', '')
                    CompanyESIAmt = detail.get('CompanyESIAmt', '')
                    CompanyBonusPrc = detail.get('CompanyBonusPrc', '')
                    CompanyBonusAmt = detail.get('CompanyBonusAmt', '')
                    PerformanceBonusAmt = detail.get('PerformanceBonusAmt', '')
                    TotalCompanyContribution = detail.get('TotalCompanyContribution', '')
                    NetSalary = detail.get('NetSalary', '')
                    ctc = detail.get('ctc', '')
                    FromDate = datetime.strptime(detail.get('FromDate', ''), "%d/%m/%Y").strftime("%Y-%m-%d") if detail.get('FromDate') else None
                    ToDate = datetime.strptime(detail.get('ToDate', ''), "%d/%m/%Y").strftime("%Y-%m-%d") if detail.get('ToDate') else None
                    CompanyBonusDeductOn = detail.get('CompanyBonusDeductOn', '')
                    CompanyEPSPrc = detail.get('CompanyEPSPrc', '')
                    CompanyEPSAmt = detail.get('CompanyEPSAmt', '')
                    CompanyEPFPrc = detail.get('CompanyEPFPrc', '')
                    CompanyEPFAmt = detail.get('CompanyEPFAmt', '')

                    insert_data = """INSERT INTO salary_structure(
                                        EmployeeID, GrossSalary, BasicSalaryPrc, BasicSalaryAmt, HRAPrc, HRAAmt, DAPrc, DAAmt, LTCPrc, LTCAmt, 
                                        EducationPrc, EducationAmt, ConveyancePrc, ConveyanceAmt, MgmtPrc, MgmtAmt, DriverPrc, DriverAmt, 
                                        MedicalAmt, OtherAmt, TotalSalary, PFDeductOn, PFPrc, PFAmt, ESIPrc, ESIAmt, ProfessionTaxAmt, IncomeTaxAmt, 
                                        DeductOtherAmt, TotalDeductionAmt, CashSalaryAmt, CompanyPFPrc, CompanyPFAmt, CompanyESIPrc, CompanyESIAmt, 
                                        CompanyBonusPrc, CompanyBonusAmt, PerformanceBonusAmt, TotalCompanyContribution, NetSalary, ctc, FromDate, 
                                        ToDate, CompanyBonusDeductOn, CompanyEPSPrc, CompanyEPSAmt, CompanyEPFPrc, CompanyEPFAmt, created, modified) 
                                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

                    data_to_insert = (
                        EmployeeID, GrossSalary, BasicSalaryPrc, BasicSalaryAmt, HRAPrc, HRAAmt, DAPrc, DAAmt, LTCPrc, LTCAmt, 
                        EducationPrc, EducationAmt, ConveyancePrc, ConveyanceAmt, MgmtPrc, MgmtAmt, DriverPrc, DriverAmt, 
                        MedicalAmt, OtherAmt, TotalSalary, PFDeductOn, PFPrc, PFAmt, ESIPrc, ESIAmt, ProfessionTaxAmt, IncomeTaxAmt, 
                        DeductOtherAmt, TotalDeductionAmt, CashSalaryAmt, CompanyPFPrc, CompanyPFAmt, CompanyESIPrc, CompanyESIAmt, 
                        CompanyBonusPrc, CompanyBonusAmt, PerformanceBonusAmt, TotalCompanyContribution, NetSalary, ctc, FromDate, 
                        ToDate, CompanyBonusDeductOn, CompanyEPSPrc, CompanyEPSAmt, CompanyEPFPrc, CompanyEPFAmt, 
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'), datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    )

                    try:
                        cur.execute(insert_data, data_to_insert)
                        db.commit()
                        print(f"Record {j+1} for EmployeeID {EmployeeID} inserted successfully.")
                    except mysql.Error as err:
                        db.rollback()
                        print(f"Error: {err} for record {j+1}: {data_to_insert}")

        cur.close()
        db.close()

        return {"message": "Salary structure data processed successfully"}

# API endpoint to fetch and store employee basic details
@app.post("/get-salary-payment/")
async def get_salary_payment():
    if data:
        db = get_db_connection()
        cur = db.cursor(dictionary=True)
        SalaryDetailID = EmployeeID = SalaryPaidDate = PayableNetSalary = TotalPaidSalary = MonthNo = MonthYear = MonthDays = PresentDays = AbsentDays = HalfDays = SalaryDays = FromDate = ToDate = WeekOff = PerDayHour = TotalDeduction = PresentDaySalaryAmt = Earnings = Deductions = ''
        for i, payment in enumerate(data):
            salary_payment = payment['SalaryPaymentDetails']
            if salary_payment:
                for j, detail in enumerate(salary_payment):
                    SalaryDetailID = detail.get('SalaryDetailID', '')
                    EmployeeID = detail.get('EmployeeID', '')
                    SalaryPaidDate = datetime.strptime(detail.get('SalaryPaidDate', ''), "%d/%m/%Y").strftime("%Y-%m-%d") if detail.get('SalaryPaidDate') else None
                    PayableNetSalary = detail.get('PayableNetSalary', '')
                    TotalPaidSalary = detail.get('TotalPaidSalary', '')
                    MonthNo = detail.get('MonthNo', '')
                    MonthYear = detail.get('MonthYear', '')
                    MonthDays = detail.get('MonthDays', '')
                    PresentDays = detail.get('PresentDays', '')
                    AbsentDays = detail.get('AbsentDays', '')
                    HalfDays = detail.get('HalfDays', '')
                    SalaryDays = detail.get('SalaryDays', '')
                    FromDate = datetime.strptime(detail.get('FromDate', ''), "%d/%m/%Y").strftime("%Y-%m-%d") if detail.get('FromDate') else None
                    ToDate = datetime.strptime(detail.get('ToDate', ''), "%d/%m/%Y").strftime("%Y-%m-%d") if detail.get('ToDate') else None
                    WeekOff = detail.get('WeekOff', '')
                    PerDayHour = detail.get('PerDayHour', '')
                    TotalDeduction = detail.get('TotalDeduction', '')
                    PresentDaySalaryAmt = detail.get('PresentDaySalaryAmt', '')
                    Earnings = json.dumps(detail.get('Earnings', ''))
                    Deductions = json.dumps(detail.get('Deductions', ''))

                    insert_data = "INSERT INTO salary_payment(SalaryDetailID, EmployeeID, SalaryPaidDate, PayableNetSalary, TotalPaidSalary, MonthNo, MonthYear, MonthDays, PresentDays, AbsentDays, HalfDays, SalaryDays, FromDate, ToDate, WeekOff, PerDayHour, TotalDeduction, PresentDaySalaryAmt, Earnings, Deductions, created, modified) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                    data_to_insert = (SalaryDetailID, EmployeeID, SalaryPaidDate, PayableNetSalary, TotalPaidSalary, MonthNo, MonthYear, MonthDays, PresentDays, AbsentDays, HalfDays, SalaryDays, FromDate, ToDate, WeekOff, PerDayHour, TotalDeduction, PresentDaySalaryAmt, Earnings, Deductions, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

                    try:
                        cur.execute(insert_data, data_to_insert)
                        db.commit()
                        print(f"Record {j+1} for EmployeeID {EmployeeID} inserted....")
                    except mysql.Error as err:
                        db.rollback()
                        print(f"Error: {err} for record {j+1}: {data_to_insert}")
        cur.close()
        db.close()

        return {"message": "Salary Payment data processed successfully"}

# API endpoint to fetch and store employee basic details
@app.post("/get-time-structure/")
async def get_time_structure():
    if data:
        db = get_db_connection()
        cur = db.cursor(dictionary=True)
        EmployeeID = InTime = OutTime = BreakMinute = LateChargeAmount = LateValidMinute = LateValidCount = LateBreakType = LateChargeBreakAmount = LateValidBreakMinute = LateValidBreakCount = LateAfterLateType = LateAfterLateChargeAmount = LateAfterLateValidMinute = LateAfterLateValidCount = EarlyGoingValidMinute = EarlyGoingValidCount = WeekoffDay = OverTimeValidMinute = OverTimeAmtPerHour = FromDate = ToDate = EarlyGoingType = EarlyGoingChargeAmount = SalaryCalculationOn = ShiftID = EmployeeWorkingMinute = OverTimeOn = WeekOffMaintainType = TotalMonthlyAllwedWeekOff = WeekOffLimitUseType = EmployeeAttendanceType = SalaryNotCalculateAfterTime = AfterMinuteComingHalfDay = ''
        for i, time in enumerate(data):
            time_structure = time['TimeStructure']
            if time_structure:
                for j, detail in enumerate(time_structure):
                    EmployeeID = detail.get('EmployeeID', '')
                    InTime = detail.get('InTime', '')
                    OutTime = detail.get('OutTime', '')
                    BreakMinute = detail.get('BreakMinute', '')
                    LateChargeAmount = detail.get('LateChargeAmount', '')
                    LateValidMinute = detail.get('LateValidMinute', '')
                    LateValidCount = detail.get('LateValidCount', '')
                    LateBreakType = detail.get('LateBreakType', '')
                    LateChargeBreakAmount = detail.get('LateChargeBreakAmount', '')
                    LateValidBreakMinute = detail.get('LateValidBreakMinute', '')
                    LateValidBreakCount = detail.get('LateValidBreakCount', '')
                    LateAfterLateType = detail.get('LateAfterLateType', '')
                    LateAfterLateChargeAmount = detail.get('LateAfterLateChargeAmount', '')
                    LateAfterLateValidMinute = detail.get('LateAfterLateValidMinute', '')
                    LateAfterLateValidCount = detail.get('LateAfterLateValidCount', '')
                    EarlyGoingValidMinute = detail.get('EarlyGoingValidMinute', '')
                    EarlyGoingValidCount = detail.get('EarlyGoingValidCount', '')
                    WeekoffDay = detail.get('WeekoffDay', '')
                    OverTimeValidMinute = detail.get('OverTimeValidMinute', '')
                    OverTimeAmtPerHour = detail.get('OverTimeAmtPerHour', '')
                    FromDate = datetime.strptime(detail.get('FromDate', ''), "%d/%m/%Y").strftime("%Y-%m-%d")
                    ToDate = datetime.strptime(detail.get('ToDate', ''), "%d/%m/%Y").strftime("%Y-%m-%d")
                    EarlyGoingType = detail.get('EarlyGoingType', '')
                    EarlyGoingChargeAmount = detail.get('EarlyGoingChargeAmount', '')
                    SalaryCalculationOn = detail.get('SalaryCalculationOn', '')
                    ShiftID = detail.get('ShiftID', '')
                    EmployeeWorkingMinute = detail.get('EmployeeWorkingMinute', '')
                    OverTimeOn = detail.get('OverTimeOn', '')
                    WeekOffMaintainType = detail.get('WeekOffMaintainType', '')
                    TotalMonthlyAllwedWeekOff = detail.get('TotalMonthlyAllwedWeekOff', '')
                    WeekOffLimitUseType = detail.get('WeekOffLimitUseType', '')
                    EmployeeAttendanceType = detail.get('EmployeeAttendanceType', '')
                    SalaryNotCalculateAfterTime = json.dumps(detail.get('SalaryNotCalculateAfterTime', {}))
                    AfterMinuteComingHalfDay = detail.get('AfterMinuteComingHalfDay', '')
                    
                    insert_data = "INSERT INTO time_structure(EmployeeID, InTime, OutTime, BreakMinute, LateChargeAmount, LateValidMinute, LateValidCount, LateBreakType, LateChargeBreakAmount, LateValidBreakMinute, LateValidBreakCount, LateAfterLateType, LateAfterLateChargeAmount, LateAfterLateValidMinute, LateAfterLateValidCount, EarlyGoingValidMinute, EarlyGoingValidCount, WeekoffDay, OverTimeValidMinute, OverTimeAmtPerHour, FromDate, ToDate, EarlyGoingType, EarlyGoingChargeAmount, SalaryCalculationOn, ShiftID, EmployeeWorkingMinute, OverTimeOn, WeekOffMaintainType, TotalMonthlyAllwedWeekOff, WeekOffLimitUseType, EmployeeAttendanceType, SalaryNotCalculateAfterTime, AfterMinuteComingHalfDay, created, modified) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                    data_to_insert = (EmployeeID, InTime, OutTime, BreakMinute, LateChargeAmount, LateValidMinute, LateValidCount, LateBreakType, LateChargeBreakAmount, LateValidBreakMinute, LateValidBreakCount, LateAfterLateType, LateAfterLateChargeAmount, LateAfterLateValidMinute, LateAfterLateValidCount, EarlyGoingValidMinute, EarlyGoingValidCount, WeekoffDay, OverTimeValidMinute, OverTimeAmtPerHour, FromDate, ToDate, EarlyGoingType, EarlyGoingChargeAmount, SalaryCalculationOn, ShiftID, EmployeeWorkingMinute, OverTimeOn, WeekOffMaintainType, TotalMonthlyAllwedWeekOff, WeekOffLimitUseType, EmployeeAttendanceType, SalaryNotCalculateAfterTime, AfterMinuteComingHalfDay, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

                    try:
                        cur.execute(insert_data, data_to_insert)
                        db.commit()
                        print(f"Record {j+1} for EmployeeID {EmployeeID} inserted....")
                    except mysql.Error as err:
                        db.rollback()
                        print(f"Error: {err} for record {j+1}: {data_to_insert}")
        cur.close()
        db.close()

        return {"message": "Time Structure data processed successfully"}