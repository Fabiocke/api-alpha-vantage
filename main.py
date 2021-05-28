import requests
from datetime import datetime, timedelta
import sqlite3

class ApiToDB():
    def __init__(self, key, db):
        self.key=key
        self.db=db
        self.tsd='https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={0}&outputsize=full&apikey='+key
        # busca a data da última semana
        self.data=(datetime.now()-timedelta(days=7)).strftime('%Y-%m-%d')
        
        
    def get_prices_week(self, ticker):
        # Consulta a api
        r=requests.get(self.tsd.format(ticker))
        # Preços
        prices=r.json()['Time Series (Daily)']
        # lista a data, ativo e preço de fechamento
        prices=[(i[0], ticker, i[1]['4. close']) for i in [*prices.items()][:list(prices).index(self.data)]]
        return prices
    
    def to_sqlite(self, prices):
        # Conexão
        conn = sqlite3.connect(self.db)
        cursor=conn.cursor()
        cursor.execute('CREATE TABLE IF NOT EXISTS daily_prices (data DATE, ativo VARCHAR(20), preco DOUBLE)')
        # Busca as datas da última semana que já estão cadastradas para o papel
        datas=cursor.execute(f"select * from daily_prices where data>={self.data} and ativo='{prices[0][1]}'").fetchall()
        datas=[i[0] for i in datas]
        # insere no db
        for price in prices:
            if price[0] in datas:
                query=f"UPDATE daily_prices SET preco={price[2]} where data = '{price[0]}' and data = '{price[1]}'"
            else:
                query=f'INSERT INTO daily_prices (data, ativo, preco) values '+str(price)
            cursor.execute(query)
            conn.commit()
        # Fecha a conexão
        conn.close()
        
    
    def upload(self, *tickers):
        for ticker in tickers:
            print('Atualizando o ticker: '+ticker)
            prices=self.get_prices_week(ticker)
            self.to_sqlite(prices)

    

if __name__=='__main__':
    key=input('Insira a chave: ')
    api=ApiToDB(key, 'prices.db')
    api.upload('B3SA3.SAO', 'PETR4.SAO')


