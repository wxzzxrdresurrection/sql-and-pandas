import pandas as pd
from config import DB

def ganancias_autores_conocidos(query, db):
    result = db.select(query)
    result['total'] = result['qty'] * result['price'] * result['royaltyper'] / 100
    res = result.groupby("au_id").sum().reset_index()
    print(res[["au_id", "total"]])
    return res[["au_id", "total"]]

def ganancias_autores_desconocidos(query2, db):
    result2 = db.select(query2)
    result2 = result2.loc[:,~result2.columns.duplicated()]
    grouped = result2.groupby(["title_id","title","price"]).sum().reset_index()
    grouped["royaltyper"] =100 - grouped["royaltyper"].fillna(0)
    grouped = grouped[grouped["royaltyper" ] > 0]
    print(grouped[["title_id", "royaltyper", "price"]])
    return grouped[["title_id", "royaltyper", "price"]]

def union_consultas(query3, db, res, grouped):
    result3 = db.select(query3)
    join = pd.merge(grouped, result3, on="title_id", how="inner")
    join["total"] = join["price"] * join["royaltyper"] * join["qty"] / 100
    res2 = join.groupby("title_id").sum().reset_index()
    res2["au_id"] = "Anonimo"
    res = res[["au_id", "total"]]
    res2 = res2[["au_id", "total"]]
    union = pd.concat([res, res2])
    print(union.groupby("au_id").sum().reset_index())
    return union.groupby("au_id").sum().reset_index()

def convert_to_excel(df, nombre, hoja):
    df.to_excel(f"{nombre}.xlsx", sheet_name=hoja)


if __name__ == '__main__':
    db = DB("localhost", "root", "admin", "pubs")

    query = """
    SELECT *
            FROM sales
        JOIN titles ON sales.title_id = titles.title_id
        JOIN titleauthor ON titles.title_id = titleauthor.title_id
    """
    query2 = """
        SELECT *
            FROM titles t
        LEFT JOIN titleauthor ta ON t.title_id = ta.title_id
    """

    query3 = """
        SELECT *
            FROM sales s"""
    

    parte1 = ganancias_autores_conocidos(query, db)
    parte2 = ganancias_autores_desconocidos(query2, db)
    union = union_consultas(query3, db, parte1, parte2)
    convert_to_excel(union, "consulta1", "gananacias_autores")
