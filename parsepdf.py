# %%
import tabula
import pandas as pd
import numpy as np
import sys

# %%
#all_case_status=['FOR FINAL HEARING','ADJOURNED CONTEMPT MATTER','NOTICE AND ADJOURNED MATTERS - CONTEMPT','URGENT ADMISSION (FRESH MATTERS)','FOR REGULAR ADMISSION',
#'NOTICE & ADJOURNED MATTERS','FOR FINAL HEARING']
all_case_status=('FOR','ADJOURNED','NOTICE','URGENT','ON THE TOP','FRESH','BOARD','POSSESSION','CANCELLATION')

# %%
#tables=tabula.read_pdf('clist.pdf',pages=[3,4,5,6,7,8,9],lattice=True)
#tables=tabula.read_pdf('clist.pdf',pages='all',lattice=True)

# %%
case_no_pattern=('R/','MCA/','CA/','CR.MA/')
def extract_case_no(casestr):
    if isinstance(casestr,str):
        l=casestr.split("\r")
        l=list(filter(lambda x:x.startswith(case_no_pattern),l))
        return ';'.join(l)
    else:
        return ""


# %%
def isBoardTypeTable(tbl_a):
    tbl=tbl_a.fillna('')
    if tbl.shape[0]==0:
        return False
    if isinstance(tbl.iloc[0,0],str):
        if tbl.iloc[0,0].startswith(('IN THE DIVISION COURT OF','IN THE COURT OF'))==True:
            return True
        else:
            return False
    else:
        return False
def getBoardDetail(tbl):
    if isBoardTypeTable(tbl):
       #print(tbl.to_markdown())
       #print("******")
       judges=' '.join(tbl.iloc[0,0].split("\r")[1:])
       board_court_d=tbl.iloc[1,0].split("COURT NO")
       board_type=board_court_d[0].strip()
       court_no=board_court_d[1].split(":")[1].strip()
       return {'judges':judges,'board_type':board_type,'court_no':court_no}
    else:
        return {'judges':'','board_type':'','court_no':''}



# %%
def isCaseListTable(tbl):
    if len(tbl.columns)>0:
        return tbl.columns[0].startswith('LIST')
    else:
        return False
def getBoardListDF(list_table,court_no,board_type,judges):
    list_tbl=list_table.copy()
    list_tbl=list_tbl.dropna(how='all')
    #print(list_tbl.to_markdown())
    list_tbl, list_tbl.columns = list_tbl[1:] , list_tbl.iloc[0]
    if len(list_tbl)>0:
        list_tbl.rename(columns=list_tbl.iloc[0]).drop(list_tbl.index[0])
        list_tbl.iloc[:,0]=list_tbl.iloc[:,0].fillna(method='ffill')
        list_tbl['srno']=list_tbl.iloc[:,0].copy()
        list_tbl['srno'].where(list_tbl['srno'].str.isdigit(),np.NaN,inplace=True)
        list_tbl['srno']=list_tbl['srno'].fillna(method='ffill')
        list_tbl['leave']=list_tbl.iloc[:,0].copy()
        list_tbl['leave']=list_tbl['leave'].fillna('')
        list_tbl['leave'].where(list_tbl['leave'].apply(lambda x:'On Leave from' in x),'',inplace=True) 

        list_tbl['status']=list_tbl.iloc[:,0].copy()
        list_tbl['status']=list_tbl['status'].fillna('')
        
        list_tbl['status']=list_tbl['status'].apply(lambda x:' '.join(x.split()))
        list_tbl['status'].where(list_tbl['status'].apply(lambda x:x.startswith(all_case_status)),np.NaN,inplace=True) # make sure list includes all possible status,othewrise wrong status

        list_tbl['status']=list_tbl['status'].fillna(method='ffill')

        list_tbl.drop(list_tbl[list_tbl['srno'].isna()].index,inplace=True)
        
        list_tbl['case_nos_b']=list_tbl['CASE DETAILS'].copy() #.apply(lambda x:extract_case_no(x))
        list_tbl['case_nos_b']=list_tbl['case_nos_b'].fillna(method='ffill')
        list_tbl['case_nos_b'].where(list_tbl['case_nos_b'].apply(lambda x:x.startswith(case_no_pattern)),np.NaN,inplace=True) #remove text which is not case no.
        list_tbl['case_nos_b']=list_tbl['case_nos_b'].fillna(method='ffill') # refill na with current case no
        list_tbl_f=list_tbl.groupby(['srno','case_nos_b'],as_index=False).agg({'status':'first','REMARKS':'first','leave':list})
        list_tbl_f['leave']=list_tbl_f['leave'].apply(lambda x:''.join(x))
        list_tbl_f['leave']=list_tbl_f['leave'].apply(lambda x: x.replace('\r',' '))

        list_tbl_f['REMARKS']=list_tbl_f['REMARKS'].fillna('na')
        list_tbl_f['REMARKS']=list_tbl_f['REMARKS'].apply(lambda x: x.replace('\r',' '))
        list_tbl_f['case_nos_b']=list_tbl_f['case_nos_b'].apply(lambda x:extract_case_no(x))
        list_tbl_f['board_type']=board_type
        list_tbl_f['court_no']=court_no
        list_tbl_f['judges']=judges
        return list_tbl_f
    else:
        return pd.DataFrame()
#getBoardListDF(tables[0],2344,'Daily Board','XYZ')
#isCaseListTable(tables[2])
#t=tables[2].dropna(how='all').append(tables[3].iloc[1:,:],ignore_index=True)
#t.iloc[18:35,:]
#x=getBoardListDF(t,2344,'Daily Board','XYZ')
#x
#x.iloc[18:35,:]
#'''
#len(tables)
#isCaseListTable(tables[2])
#tables[2]
#getBoardListDF(tables[2],2344,'Daily Board','XYZ')
#.dropna(how='all')
def getBoardTypeTables(tbls):
    tbl_list=[]
    
    for tbl_a in tbls:
        tbl=tbl_a.copy()
        #print(tbl.columns.values)
        first_col=tbl.columns.values[0]
        if isBoardTypeTable(tbl)==True:
            #print(tbl.to_markdown())
            #print("**************")
            tbl.columns=['col1','col2']
            tbl_list.append(tbl)
        if "COURT NO" in first_col or "HONOURABLE" in first_col:
            #print(tbl.to_markdown())
            tmp_tbl=tbl_list.pop()
            tbl=tbl.T.reset_index().T
            tbl.columns=['col1','col2']
            tbl_list.append(pd.concat([tmp_tbl,tbl],axis=0,ignore_index=True))
    return tbl_list
        
def createCaseTblListGroup(tbls):
    tbl_list=[]
    group_list=[]
    for tbl_a in tbls:

        if isCaseListTable(tbl_a)==True:
            tbl_list.append(tbl_a)
        else:
            if len(tbl_list)>0:
                group_list.append(tbl_list.copy())
                tbl_list=[]
    group_list.append(tbl_list)
    return group_list
def parseAndSave(infilename,outfilename):
    tables=tabula.read_pdf(infilename,pages='all',lattice=True)
    bgroups=getBoardTypeTables(tables)
    #print(bgroups[1].to_markdown())
    #getBoardDetail(t[1])
    cgroups=createCaseTblListGroup(tables)
    parsed_tables=[]
    for i in range(len(bgroups)):
        boardData=getBoardDetail(bgroups[i])
        df=pd.concat(cgroups[i],axis=0,ignore_index=True)
        if len(df)>0:
            caselistdf=getBoardListDF(df,boardData['court_no'],boardData['board_type'],boardData['judges'])
            if len(caselistdf)>0:
                parsed_tables.append(caselistdf.sort_values(by=['srno']))
    pd.concat(parsed_tables).to_csv(outfilename)
# %%
def main():
    args=sys.argv[1:]
    if len(args)!=2:
        print("Invalid argument. Expected python parsepdf.py INPUTFILE.pdf OUTPUTFILE.csv")
        return
    in_file=args[0]
    outfile=args[1]
    parseAndSave(in_file,outfile)

    # for tbl in tables:
    #     if isBoardTypeTable(tbl)==True:
    #         #print("boardtype")
    #         if len(temp_buff_group)>0:
    #             main_group.append(temp_buff_group.copy())
    #         temp_buff_group=[tbl]
    #     if isCaseListTable(tbl)==True:
    #         #print("listtype")
    #         temp_buff_group.append(tbl)
    # main_group.append(temp_buff_group.copy())
    # parsed_tables=[]
    # #tables[0]
    # # len(tables)
    # # print(len(main_group[0]))
    # #main_group[1]

    # #boardData=getBoardDetail(main_group[1][0])
    # #getBoardListDF(main_group[1][2],boardData['court_no'],boardData['board_type'],boardData['judges'])
    # for g in main_group:
    #     df_with_data_rows=list(map(lambda x:x.iloc[1:,:],g[2:]))
    #     #print(df_with_data_rows)
    #     #now merge first list_df with df_with_data_rows
    #     clist_df=g[1]
    #     if len(g)>2:
    #         clist_df=pd.concat([g[1]]+df_with_data_rows,ignore_index=True)
    #     boardData=getBoardDetail(g[0])
    #     caselistdf=getBoardListDF(clist_df,boardData['court_no'],boardData['board_type'],boardData['judges']).sort_values(by=['srno'])
    #     parsed_tables.append(caselistdf)
    # #len(parsed_tables)
    # pd.concat(parsed_tables).to_csv(outfile)

# import glob


if __name__ == "__main__":
    main()
#isBoardTypeTable(tables[3])
#tables[3].iloc[1:,:]


        



