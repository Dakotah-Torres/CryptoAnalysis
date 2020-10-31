from pycoingecko import CoinGeckoAPI
import pandas as pd
from pandas import DataFrame as DF
import numpy as np 
import datetime
import nbformat
import plotly.graph_objects as go
import os
import shutil 



#Setting up the INIT Variables 
my_exchange_list = ["Coinbase Pro", "KuCoin"]
cg = CoinGeckoAPI()

def trading_pair_df(clean_exchange_data):
    clean_exchange_data['target']=clean_exchange_data['target'].replace(to_replace='USDT', value='USD')
    return pd.concat([clean_exchange_data['coin_id'], clean_exchange_data['target']], axis=1)

def get_exchange_data(exchange_list=[]):
    if exchange_list == []:
        print("please give a list of exchanges")
    try: 
        exchanges = pd.DataFrame(cg.get_exchanges_list())
        
        My_Exchnages = exchanges[exchanges['name'].isin(exchange_list)]
        My_Exchanges = My_Exchnages.drop(columns=['year_established', 'country' ,'description', 'url', 'image', 'has_trading_incentive', 'trust_score', 'trust_score_rank'])

        tickers = [cg.get_exchanges_tickers_by_id(x) for x in My_Exchanges['id']]
        return DF(tickers)
    except: 
        print("something went wrong")

def clean_exchange_data(exchange_df):
    #Creates new df for the converted columes
    converted_Vol = pd.DataFrame(list(exchange_df['converted_volume']))
    #Drops the unnesicary columes
    exchange_df_droped = exchange_df.drop(columns=['converted_last', 'market' ,'trust_score', 'timestamp', 'last_traded_at', 'is_anomaly', 'is_stale', 'trade_url', 'converted_volume'])
    #joins the two tables
    exchange_df_clean = pd.concat([ exchange_df_droped, converted_Vol], axis=1, join= 'outer')
    return  exchange_df_clean

def get_coin_ohlc_and_chart(trading_pair_df, record_number=0, days='1'):
    #fetechs the ohlc from API
    try:
        ohlc = pd.DataFrame(cg.get_coin_ohlc_by_id(trading_pair_df['coin_id'][record_number], trading_pair_df['target'][record_number].lower(), days))

        #change the Colume Headings
        ohlc.columns = ['Time', 'Open', 'High', 'Low', 'Close']

        #changes the time stamp data to proper times format
        ohlc['Time'] = ohlc['Time'].apply(lambda x : datetime.datetime.fromtimestamp(x/1000))

        #creates a figure object
        #fig = go.Figure(data = [go.Candlestick(x=ohlc['Time'], open=ohlc['Open'], high=ohlc['High'], low=ohlc['Low'], close=ohlc['Close'])])

        return ohlc #, fig

    except:
       pass

def get_coin_ohlc_and_chart_tuple(tuple_list, record_number=0, days='1'):
    #fetechs the ohlc from API
    try:
        ohlc = pd.DataFrame(cg.get_coin_ohlc_by_id(tuple_list[0][record_number], tuple_list[1][record_number].lower(), days))

        #change the Colume Headings
        ohlc.columns = ['Time', 'Open', 'High', 'Low', 'Close']

        #changes the time stamp data to proper times format
        ohlc['Time'] = ohlc['Time'].apply(lambda x : datetime.datetime.fromtimestamp(x/1000))

        #creates a figure object
        #fig = go.Figure(data = [go.Candlestick(x=ohlc['Time'], open=ohlc['Open'], high=ohlc['High'], low=ohlc['Low'], close=ohlc['Close'])])

        return ohlc, #fig
    except:
        pass

def ohlc_to_csv(trading_pair_df,dir_name='Defualt_Output_For_Exchange', number_of_records=10, days='1'):
    errors = 0
    passes = 0
    
    exchange_folder_path = "{current}/Exchanges".format(current=os.getcwd())
    error_log_path = "{current}/Exchanges/Error_Logs".format(current=os.getcwd())
    path = "{current}/Exchanges/{dir_name}/".format(current=os.getcwd(), dir_name=dir_name)


    #confirms that the user is not requesting more records then are there
    if number_of_records > len(trading_pair_df):
        recs=str(len(trading_pair_df))
        print("Requested Number {x}  Records Avliable {y}".format(x=number_of_records), y=recs)
    
    #setting up Initital Folder Stucture
    try:
        #creating the Echange Dir
        os.mkdir(exchange_folder_path)
        #makes Error Log Dir for all errors
        os.mkdir(error_log_path)
    except Exception as e:
        print(e)
    
    #check the Directory list to make the folders needed
    try:
        os.mkdir(path)
    except Exception as e:
        error_log = "{path}/Errors.log".format(path=error_log_path)
        error_messaage =  "{Exchange}: Error: {err}\n".format(err=e, Exchange=dir_name)
        f = open(error_log, "a")
        f.write(error_messaage)
        f.close()
        
       

    for i in trading_pair_df.index: 
        if i < number_of_records:
            try:
                #Setting Up the File Stucture
                #creating the file names
                file_name = "{base}_{target}_{time_period}_days.csv".format(base=trading_pair_df['coin_id'][i], target=trading_pair_df['target'][i].lower(), time_period=days)
                url = path + file_name

                #fetching the data 
                ohlc = get_coin_ohlc_and_chart(trading_pair_df, i, days)
                ohlc_csv = ohlc.to_csv() 

                #creating the files
                f = open(url, "w+")
                f.write(ohlc_csv)
                f.close()
                passes += 1
            except Exception as e:
                errors += 1
                error_log = "{path}/Errors.log".format(path=error_log_path)
                error_messaage =  "error at: {file_name} with Error {err}\n".format(file_name=file_name, err=e)
                f = open(error_log, "a")
                f.write(error_messaage)
                f.close()
                pass

    percent_success = float(passes/number_of_records)*100
    process_info = "Errors:{errs}\nPassess:{passes}\nPercent Success: %{PS}".format(errs=errors, passes=passes, PS=percent_success)
    print('=====================================================')
    print('     Data Extraction Summary For {EX}                '.format(EX=dir_name))
    print('=====================================================')
    print(process_info)
    print('=====================================================')

def clean_up():
    exchange_folder_path = "{current}/Exchanges".format(current=os.getcwd())
    # cleans up previous analysis folders and files
    try:
        shutil.rmtree(exchange_folder_path)
    except Exception as e:
        print(e)
        pass






def main():

    #Here we are cleaning up the previous analysys Data 
    clean_up()

    # These are the functions to get the Exchanges tickers and filaing them into there Data Frames for each Exchange
    exchange_data = get_exchange_data(my_exchange_list)
    exchange_data_CB = DF(exchange_data['tickers'][0])
    exhhange_data_KC = DF(exchange_data['tickers'][1])

    # This Cleans the data so that it can be more easily worked with
    clean_KC_data = clean_exchange_data(exhhange_data_KC)
    clean_CB_data = clean_exchange_data(exchange_data_CB)

    # this seperates the information into the trading pairs
    trading_pairs_KC = trading_pair_df(clean_KC_data)
    trading_pairs_CB = trading_pair_df(clean_CB_data)


    #This is where the CSV files will be created for each echanges and send the datat that they have for analys
    records = int(input("How Many Records Would you like> "))
    days = input("How Many Days (ie. 1, 7, 14, 30)> ")
    
    trading_pairs = [trading_pairs_CB, trading_pairs_KC ]
    for x in range(len(trading_pairs)):
        ohlc_to_csv(trading_pairs[x], my_exchange_list[x], records, days)


if __name__ == "__main__":
    main()