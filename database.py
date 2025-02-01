import psycopg2


class Portfolio_DB:
    connection = None
    cursor = None
    
    def __init__(self):
        try:
            self.connection = psycopg2.connect(
                
            )
            print("Таблица создана1")
            self.cursor = self.connection.cursor()            
        except Exception as e:
            print(f"Error connecting to the database: {e}")


    def close(self):
        self.cursor.close()
        self.connection.close()
    
    
    def add_user(self, user_id):
        try:
            self.cursor.execute("""
                    INSERT INTO users (user_id)
                    VALUES (%s,)
                    ON CONFLICT DO NOTHING;
                """, (user_id, ))
            return True
        except Exception as e:
            print(f"Error inserting data into database table users: {e}")
            self.connection.rollback()
            return False
            
            
    def add_portfolio(self,user_id):
        try:
            self.cursor.execute("""
                    INSERT INTO portfolios (user_id)
                    VALUES (%s,)
                    ON CONFLICT DO NOTHING;
                """, (user_id, ))
            return True
        except Exception as e:
            print(f"Error inserting data into database table portfolios: {e}")
            self.connection.rollback()
            return False
            
            
    def add_portfolio_assets(self,portfolio_id, asset_id, quantity, purchase_price, purchase_date):
        try:
            self.cursor.execute("""
                    INSERT INTO portfolio_assets (portfolio_id, asset_id, quantity, purchase_price, purchase_date)
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING;
                """, (portfolio_id, asset_id, quantity, purchase_price, purchase_date))
            return True
        except Exception as e:
            print(f"Error inserting data into database table portfolio_assets: {e}")
            self.connection.rollback()
            return False
            
            
    def get_asset_by_ticker(self, user_input):
        self.cursor.execute("SELECT id, name, ticker FROM assets WHERE ticker = %s", (user_input,))
        exact_match = self.cursor.fetchone()
        
        if exact_match:
            return {"data": "True"}  
        
        self.cursor.execute("""
            SELECT ticker FROM assets 
            WHERE similarity(ticker, %s) > 0.3 
            ORDER BY similarity(ticker, %s) DESC 
            LIMIT 10;
        """, (f"%{user_input}%", f"%{user_input}%"))
        
        similar = self.cursor.fetchall()
        
        if similar:
            return {"data": similar}  
        else:
            return {"data": "False"} 
    
    
    def get_asset_id(self, ticker):
        try:
            self.cursor.execute("SELECT id FROM assets WHERE ticker = %s", (ticker,))
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            print(f"Error fetching asset ID: {e}")
            return None