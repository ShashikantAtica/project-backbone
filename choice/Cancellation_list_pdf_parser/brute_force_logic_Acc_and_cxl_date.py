from PyPDF2 import PdfReader
import json

if __name__ == '__main__':
    reader = PdfReader("report_1p.pdf")

    #These two lists will be storing account number and cancellation date
    account=list()
    cxl_date=list()
    for page in reader.pages:

        textt=page.extract_text()
        textt = textt.replace("\n", " ")
        
        flag=0

        #Extracting account numbers
        for idx in range(0, len(textt)):
            next_nine_char_str=textt[idx : idx+9]
            if(next_nine_char_str.isnumeric()):
                account.append(next_nine_char_str)
                idx+=9

        #Extracting cancellation dates
        for idx in range(0, len(textt)-5):
            if((textt[idx]=='/' and textt[idx+2]=='/')):
                if(flag==1 ):
                    if(textt[idx-2].isnumeric()):
                        cxl_date.append(textt[idx-2:idx+5])
                    else:
                        cxl_date.append(textt[idx-1:idx+5])
                flag=flag^1
            elif((textt[idx]=='/' and textt[idx+3]=='/')):
                if(flag==1 ):
                    if(textt[idx-2].isnumeric()):
                        cxl_date.append(textt[idx-2:idx+6])
                    else:
                        cxl_date.append(textt[idx-1:idx+6])
                flag=flag^1
        #To remove the extra one date coming through footer/description of EVERY page        
        cxl_date.pop()
    

    if(len(account)!=len(cxl_date)):
        print("Data is mismatched")
            
    print("Total number of cancellation", len(account))

    #changing format of date
    for idx in range(0, len(cxl_date)):
        temp_date=""
        n=len(cxl_date[idx])
        if(n==8):
            temp_date="20"+cxl_date[idx][n-2:n]+"-"+cxl_date[idx][0:2]+"-"+cxl_date[idx][3:5]
        if(n==6):
            temp_date="20"+cxl_date[idx][n-2:n]+"-0"+cxl_date[idx][0:1]+"-0"+cxl_date[idx][2:3]
        if(n==7 and cxl_date[idx][1]=='/'):
            temp_date="20"+cxl_date[idx][n-2:n]+"-0"+cxl_date[idx][0:1]+"-"+cxl_date[idx][2:4]
        if(n==7 and cxl_date[idx][2]=='/'):
            temp_date="20"+cxl_date[idx][n-2:n]+"-"+cxl_date[idx][0:2]+"-"+cxl_date[idx][3:4]
        cxl_date[idx]=temp_date
            


    # Json data for Account and cancellation date :
    data_dic=dict()
    data_dic={"data":[]}
    for idx in range(0, len(cxl_date)):
        temp_dic={"Account":account[idx],"CxlDate":cxl_date[idx]}
        data_dic["data"].append(temp_dic)

    json_data=json.dumps(data_dic)
    print(json_data)

# // yyyy-mm-dd 2023-09-01
    