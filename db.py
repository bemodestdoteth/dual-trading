import sqlite3
import os

def create_dual_trading_db():
    if not(os.path.isfile("./strats.db")):
        con = sqlite3.connect('strats.db')
        cur = con.cursor()
        # Create table
        cur.execute("CREATE TABLE dual_trading (id PRIMARY KEY, coin NOT NULL, price NOT NULL, amount NOT NULL, date NOT NULL, exchange NOT NULL, order_no, final_price, settled NOT NULL, below_settlement NOT NULL)")
        con.commit()
        con.close()
def remove_dual_trading_db():
    con = sqlite3.connect('strats.db')
    cur = con.cursor()
    # Create table
    cur.execute("DROP TABLE dual_trading")
    con.close()
def insert_trading_strat(strat):
    con = sqlite3.connect('strats.db')
    cur = con.cursor()
    cur.execute("INSERT INTO dual_trading VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", strat)
    con.commit()
    con.close()
def get_trading_strat(id):
    con = sqlite3.connect('strats.db')
    cur = con.cursor()
    res = cur.execute("SELECT * FROM dual_trading WHERE id = ?", (id, )).fetchone()
    con.close()
    return res
def get_all_trading_strats():
    con = sqlite3.connect('strats.db')
    cur = con.cursor()
    res = cur.execute("SELECT * FROM dual_trading").fetchall()
    con.close()
    return res
def get_not_settled_strats():
    con = sqlite3.connect('strats.db')
    cur = con.cursor()
    res = cur.execute("SELECT * FROM dual_trading WHERE settled = 0").fetchall()
    con.close()
    return res
def update_trading_strat(id, args):
    con = sqlite3.connect('strats.db')
    cur = con.cursor()

    # Replace empty input as previous data
    prev_data = get_trading_strat(id)
    for i in range(len(args)):
        if args[i] == "":
            args[i] = prev_data[i]
    args.append(id)

    query = "UPDATE dual_trading SET coin = ?, price = ?, amount = ?, date = ?, exchange = ? WHERE id = ?"
    cur.execute(query, tuple(args))
    con.commit()
    con.close()
def insert_final_price(id, final_price):
    con = sqlite3.connect('strats.db')
    cur = con.cursor()
    query = "UPDATE dual_trading SET final_price = ? WHERE id = ?"
    cur.execute(query, (final_price, id))
    con.commit()
    con.close()
def update_order_number(id, order_no):
    con = sqlite3.connect('strats.db')
    cur = con.cursor()
    query = "UPDATE dual_trading SET order_no = ? WHERE id = ?"
    cur.execute(query, (order_no, id))
    con.commit()
    con.close()
def settle_trade(id):
    con = sqlite3.connect('strats.db')
    cur = con.cursor()
    query = "UPDATE dual_trading SET settled = 1 WHERE id = ?"
    cur.execute(query, (id, ))
    con.commit()
    con.close()
def toggle_margin(id, below_settlement):
    con = sqlite3.connect('strats.db')
    cur = con.cursor()
    query = "UPDATE dual_trading SET below_settlement = ? WHERE id = ?"
    cur.execute(query, (below_settlement, id))
    con.commit()
    con.close()
    
if __name__ == "__main__":
    remove_dual_trading_db()
    create_dual_trading_db()