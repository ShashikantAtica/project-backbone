from PyPDF2 import PdfReader
# import numpy as np

# def acc_cxldate_puller(str):

if __name__ == '__main__':
    reader = PdfReader("report_22P.pdf")
    
    account=list()
    cxl_date=list()
    

    for page in reader.pages:
        strt_end_idx_each_booking=[]
        textt=page.extract_text()
        textt = textt.replace("\n", " ")
        strt_idx=-1

        #Storing starting and ending index for each booking's string
        for idx in range(0, len(textt)):
            next_nine_char_str=textt[idx : idx+9]
            if(next_nine_char_str.isnumeric()):
                if(strt_idx==-1):
                    strt_idx=idx;
                else:
                    strt_end_idx_each_booking.append([strt_idx, idx-1])
                    strt_idx=idx
                idx+=8

        end_idx=textt.find("Total Cancellations", strt_idx)
        strt_end_idx_each_booking.append([strt_idx,end_idx-1])

        # priting each string
        for i in range(0, len(strt_end_idx_each_booking)):
              s_idx=strt_end_idx_each_booking[i][0]
              e_idx=strt_end_idx_each_booking[i][1]
              print(textt[s_idx : e_idx+1])
        
        # #Extracing account number
        # for i in range(0, len(strt_end_idx_each_booking)):
        #     s_idx=strt_end_idx_each_booking[i][0]
        #     e_idx=strt_end_idx_each_booking[i][1]
        #     account.append(textt[s_idx : e_idx+1])


        # Indexing will be Account Guest_Name Company Arrival_Group Nights Rate_Plan GTD Source Rm Type Cxl Code Cxl Date Cxl Clk

    


