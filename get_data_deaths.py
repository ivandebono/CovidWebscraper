import requests
import dateparser
import urllib
from bs4 import BeautifulSoup
import pandas as pd
import datetime as dt
import numpy as np



def get_deaths_France():
    base_url='https://www.insee.fr/fr/statistiques/4487988'
    sourcePage = urllib.request.urlopen(base_url)
    soup = BeautifulSoup(sourcePage.read())
    links = soup.find_all('a')
    links = filter(lambda x: x.has_attr('href'), links)

    for link in links:

        href = link['href']
        if 'deces_quotidiens_departement_csv.zip' in href:        
            url='https://www.insee.fr'+href


    import dateparser

    df=pd.read_csv(url,compression='zip',encoding='latin1',sep=';',
                   usecols=['Date_evenement','Zone','Total_deces_2020','Total_deces_2019','Total_deces_2018'])
    df.drop(df[df.Zone!='France'].index,inplace=True)
    df['date'] = df.Date_evenement.apply(dateparser.parse)

    deaths=df.Total_deces_2020.diff().fillna(df.Total_deces_2020)
    df['deaths2018']=df.Total_deces_2018.diff().fillna(df.Total_deces_2018)
    df['deaths2019']=df.Total_deces_2019.diff().fillna(df.Total_deces_2019)
    deathsavg=df[['deaths2018','deaths2019']].mean(axis=1)
    deathsmin=df[['deaths2018','deaths2019']].min(axis=1)
    deathsmax=df[['deaths2018','deaths2019']].max(axis=1)
    FR_allcause=pd.DataFrame({'date':df.date,'deaths':deaths,'deathsavg':deathsavg,
                              'deathsmin':deathsmin,'deathsmax':deathsmax})



    ###
    ### Get historical data and merge
    ###

    url='https://www.insee.fr/fr/statistiques/fichier/3596204/T80DEC.xls'

    FRhist=pd.read_excel(url,skiprows=3,skipfooter=1,sheet_name='FM').drop([0])

    FR3yr=FRhist[FRhist['MOIS'].isin(['2015','2016','2017'])]

    FR3yr=(FR3yr.set_index('MOIS').T)
    FR3yr.drop('Année',inplace=True)




    FR3yr['month']=FR3yr.index
    FR3yr.reset_index(drop=True,inplace=True)
    FR3yr

    ### !!!! These are DAILY DEATHS in every month



    def parse_French_month(df,year):
    
        dfyr=df[[str(year),'month']]

        dfyr['month']=(dfyr.month.str.strip()+' '+str(year)).apply(lambda x: dateparser.parse(x,settings={'PREFER_DAY_OF_MONTH': 'first'}).date())

        dfyr['date']=pd.to_datetime(dfyr.month)
        return dfyr



    def INSEE_monthly2daily(df):

        dailies=pd.DataFrame({'date':(pd.period_range(min(df.date),
                                dt.datetime(df.date.dt.year.unique()[0],12,31))).to_timestamp()})
        df2=pd.merge(df,dailies,on='date',how='outer').sort_values(by='date').ffill()
    
        #df2=pd.DataFrame({'date':df2.date,'deaths':df2[str(df.date.dt.year.unique()[0])]})
    
        return df2

    FR15=INSEE_monthly2daily(parse_French_month(FR3yr,'2015'))
    FR16=INSEE_monthly2daily(parse_French_month(FR3yr,'2016'))
    FR17=INSEE_monthly2daily(parse_French_month(FR3yr,'2017'))

    FR_allcause1=pd.merge(FR_allcause, FR15, left_on=[FR_allcause.date.dt.month, FR_allcause.date.dt.day],
                                right_on=[FR15.date.dt.month, FR15.date.dt.day],how='inner')
    FR_allcause1.drop(columns=['key_0','key_1'],inplace=True)
    FR_allcause1.rename(columns={'date_x':'date'},inplace=True)
    FR_allcause1

    FR_allcause2=pd.merge(FR_allcause1, FR16, left_on=[FR_allcause1.date.dt.month, FR_allcause1.date.dt.day],
                                 right_on=[FR16.date.dt.month, FR16.date.dt.day])
    FR_allcause2.drop(columns=['key_0','key_1'],inplace=True)
    FR_allcause2.rename(columns={'date_x':'date'},inplace=True)
    FR_allcause2

    FR_allcause3=pd.merge(FR_allcause2, FR17, left_on=[FR_allcause2.date.dt.month, FR_allcause2.date.dt.day],
                                 right_on=[FR17.date.dt.month, FR17.date.dt.day])
    FR_allcause3.drop(columns=['key_0','key_1'],inplace=True)
    FR_allcause3.rename(columns={'date_x':'date'},inplace=True)
    FR_allcause3



    deathsavg=FR_allcause3[['deathsavg','2015','2016','2017']].mean(axis=1)
    deathsmin=FR_allcause3[['deathsmin','2015','2016','2017']].min(axis=1)
    deathsmax=FR_allcause3[['deathsmin','2015','2016','2017']].max(axis=1)


    FR_allcause=pd.DataFrame({'date':FR_allcause.date,'deaths':FR_allcause.deaths,'deathsavg':deathsavg,
                             'deathsmin':deathsmin,'deathsmax':deathsmax})
    
    return FR_allcause



def get_deaths_Portugal():

    url='https://github.com/dssg-pt/dados-SICOeVM/raw/master/mortalidade.csv'

    df=pd.read_csv(url,usecols=['Data','geral_pais'])
    df['Data']=pd.to_datetime(df.Data,format='%d-%m-%Y')

    df19=df[df.Data.dt.year==2019]

    df18=df[df.Data.dt.year==2018]
    df17=df[df.Data.dt.year==2017]
    df16=df[df.Data.dt.year==2016]
    df15=df[df.Data.dt.year==2015]

    df20=df[df.Data.dt.year==2020]
    df20.rename(columns={'geral_pais':'deaths','Data':'date'},inplace=True)

    def create_day_month(df,datename):
        df['day'] = df[datename].dt.day
        df['month'] = df[datename].dt.month

        return df

    data_frames = [df15,df16,df17,df18,df19]

    for dataframe in data_frames:
        dataframe=create_day_month(dataframe,'Data')

    from functools import reduce
    df_merged = reduce(lambda  left,right: pd.merge(left,right,how='outer', on=['month', 'day']), data_frames)

    df_merged['deathsavg']=df_merged[['geral_pais_x','geral_pais_x','geral_pais_y','geral_pais']].mean(axis=1)
    df_merged['deathsmin']=df_merged[['geral_pais_x','geral_pais_x','geral_pais_y','geral_pais']].min(axis=1)
    df_merged['deathsmax']=df_merged[['geral_pais_x','geral_pais_x','geral_pais_y','geral_pais']].max(axis=1)



    df_merged.interpolate(inplace=True)

    #df_merged[(df_merged.day==29) & (df_merged.month==2)]

    create_day_month(df20,'date')
    df_merged=pd.merge(df20,df_merged,on=['day','month'],how='outer')

    PT_allcause=df_merged[['date','deaths','deathsavg','deathsmin','deathsmax']]
    
    return PT_allcause


def get_deaths_Sweden():

    url='https://www.scb.se/hitta-statistik/statistik-efter-amne/befolkning/befolkningens-sammansattning/befolkningsstatistik/pong/tabell-och-diagram/preliminar-statistik-over-doda'

    import dateparser

    dateparse = lambda dates: [dateparser.parse(d,date_formats=['%d %B']) for d in dates]

    SE=pd.read_excel(url,sheet_name='Tabell 1',skiprows=6,parse_dates=['DagMånad'],
                     date_parser=dateparse)
    SE.rename(columns={'DagMånad':'date'},inplace=True)

    fiveyears=['2015','2016','2017','2018','2019']
    SE[fiveyears]=SE[fiveyears].replace(['0', 0], np.nan)

    SE['deathsmin']=SE[fiveyears].min(axis=1)
    SE['deathsmax']=SE[fiveyears].max(axis=1)
    SE.rename(columns={'2015-2019':'deathsavg','2020':'deaths'},inplace=True)

    SE_allcause=SE[['date','deaths','deathsavg','deathsmin','deathsmax']]
    #SE_allcause.dropna(inplace=True)
    
    return SE_allcause



def get_deaths_Italy():
    
    import requests
    from io import BytesIO
    from zipfile import ZipFile

    #url='https://www.istat.it/it/files//2020/03/Dataset-decessi-comunali-giornalieri-e-tracciato-record_22ottobre2020.zip'

    url='https://www.istat.it/it/files//2020/03/Dataset-decessi-comunali-giornalieri-e-tracciato-record_dati-al-30-settembre.zip'
    resp=urllib.request.urlopen(url)

    zipfile = ZipFile(BytesIO(resp.read()))
    for f in list(zipfile.filelist):
        if '.csv' in f.filename:
            extracted_file = zipfile.open(f.filename)
    df=pd.read_csv(extracted_file,sep=',',usecols=['GE','CL_ETA','T_15','T_16','T_17','T_18','T_19','T_20'],dtype={'GE': object})
    df['yyyymmdd']=pd.to_datetime('2020'+df.GE,format='%Y%m%d')
    df['T_20']=pd.to_numeric(df.T_20,errors='coerce')
    #deaths2020=df[['yyyymmdd','T_20']]
    df=df.groupby(by='yyyymmdd',as_index=False).sum()

    #df

    deathsavg=df[['T_15','T_16','T_17','T_18','T_19']].mean(axis=1)
    deathsmin=df[['T_15','T_16','T_17','T_18','T_19']].min(axis=1)
    deathsmax=df[['T_15','T_16','T_17','T_18','T_19']].max(axis=1)

    IT_allcause=pd.DataFrame({'date':df.yyyymmdd,'deaths':df.T_20,'deathsavg':deathsavg,'deathsmin':deathsmin,'deathsmax':deathsmax})
    IT_allcause.loc[IT_allcause.date == dt.datetime(2020,2,29),['deathsavg','deathsmin','deathsmax']]=np.nan



    IT_allcause.interpolate(inplace=True)
    

    return IT_allcause


def get_deaths_Germany():
    #https://www.destatis.de/DE/Themen/Gesellschaft-Umwelt/Bevoelkerung/Sterbefaelle-Lebenserwartung/Tabellen/sonderauswertung-sterbefaelle.html


    url='https://www.destatis.de/DE/Themen/Gesellschaft-Umwelt/Bevoelkerung/Sterbefaelle-Lebenserwartung/Tabellen/sonderauswertung-sterbefaelle.xlsx?__blob=publicationFile'
    #https://www.destatis.de/EN/Press/2020/11/PE20_472_12621.ht
    df=pd.read_excel(url,sheet_name='D_2016_2020_Tage',skiprows=7).T

    df['date']=pd.to_datetime(df[0].astype(str)+'2020',format='%d.%m.%Y',errors='coerce')
    #df.columns=['date',2020,2019,2018,2017,2016]

    df.rename(columns={1:'deaths',2:'2019',3:'2018',4:'2017',5:'2016'},inplace=True)
    df.drop('Jahr',inplace=True)
    #df.rename(columns=str).rename(columns={'NaN':'date'},inplace=True)
    for name in ['2019','2018','2017','2016']:
        df[name]=pd.to_numeric(df[name],errors='coerce').interpolate()

    df['deathsavg']=df[['2019','2018','2017','2016']].mean(axis=1)
    df['deathsmin']=df[['2019','2018','2017','2016']].min(axis=1)
    df['deathsmax']=df[['2019','2018','2017','2016']].max(axis=1)

    DE_allcause=df.drop(['Unnamed: 367'])
    
    return DE_allcause